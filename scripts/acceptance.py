#!/usr/bin/env python3
"""Run disposable full-NiFi acceptance tests using synthetic OPC UA data."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import secrets
import re
import signal
import subprocess
import time
import urllib.request
import urllib.error

ROOT = Path(__file__).resolve().parents[1]
COMMAND_TIMEOUT_SECONDS = 120
CLEANUP_TIMEOUT_SECONDS = 30
CONTENT_TAG_LIMIT = 16
CONTENT_FLOWFILE_LIMIT = 80
CONTENT_RECORD_LIMIT = 2048
CONTENT_BYTE_LIMIT = 4 * 1024 * 1024
CONTENT_CAPTURE_SECONDS = 30
INDEXED_VALUE_STRIDE = 10000
SAMPLE_HISTORY_LIMIT = 256
SOAK_PROGRESS_SECONDS = 10


def command(*args):
    try:
        return subprocess.check_output(list(args), text=True, stderr=subprocess.STDOUT,
                                       timeout=COMMAND_TIMEOUT_SECONDS).strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f'{args[0]} {args[1]} failed: {e.output[-2000:]}') from None
    except subprocess.TimeoutExpired:
        raise RuntimeError(f'{args[0]} {args[1]} timed out; limit={COMMAND_TIMEOUT_SECONDS}s') from None


def harness_provenance():
    result = {'harnessCommit': None,
              'harnessSha256': hashlib.sha256(Path(__file__).resolve().read_bytes()).hexdigest(),
              'sourceTreeDirty': None}
    try:
        top = Path(command('git', '-C', str(ROOT), 'rev-parse', '--show-toplevel')).resolve()
        if top != ROOT.resolve():
            raise RuntimeError('The source directory is not a Git repository root')
        commit = command('git', '-C', str(ROOT), 'rev-parse', 'HEAD')
        dirty = bool(command('git', '-C', str(ROOT), 'status', '--porcelain', '--untracked-files=normal'))
        result.update({'harnessCommit': commit, 'sourceTreeDirty': dirty})
    except (OSError, RuntimeError, ValueError) as error:
        result['harnessProvenanceError'] = str(error)[:500]
    return result


def timestamp(value):
    # Java Instant can emit nanoseconds; Python 3.9/3.10 accepts microseconds.
    value = re.sub(r"(\.\d{6})\d+", r"\1", value)
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def validate_fresh_records(records, now=None):
    # Load.Tag values change once per second. A new callback containing an old
    # source value does not demonstrate that this synthetic server recovered.
    now = time.time() if now is None else now
    assert records, 'No records to validate'
    result = {}
    for field in ('sourceTimestamp', 'receivedTimestamp'):
        assert all(isinstance(r.get(field), str) and r[field] for r in records), f'Missing {field}'
        times = [timestamp(r[field]) for r in records]
        assert now - min(times) < 30, f'Stale {field} does not prove recovery'
        assert max(times) <= now + 5, f'Future {field}; check test host clocks'
        result['oldest' + field[0].upper() + field[1:]] = min(times)
        result['newest' + field[0].upper() + field[1:]] = max(times)
    return result


def resource_snapshots(diagnostics, standalone_node):
    keys = ['usedHeap', 'usedHeapBytes', 'totalThreads', 'daemonThreads']
    if diagnostics.get('nodeSnapshots'):
        return {n['address']: {k: n['snapshot'].get(k) for k in keys}
                for n in diagnostics['nodeSnapshots']}
    return {standalone_node: {k: diagnostics.get('aggregateSnapshot', {}).get(k) for k in keys}}


def positive_limit(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError('Limit must be a positive finite number') from None
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError('Limit must be a positive finite number')
    return number


def resource_budget_report(nodes, max_heap_mib=None, max_threads=None):
    if not nodes:
        raise ValueError('Resource budgets require an expected NiFi node')
    max_heap_mib = None if max_heap_mib is None else positive_limit(max_heap_mib)
    max_threads = None if max_threads is None else positive_limit(max_threads)
    limits = {'maxHeapMiB': max_heap_mib, 'maxThreads': max_threads}
    heap_limit = None
    if max_heap_mib is not None:
        numerator, denominator = max_heap_mib.as_integer_ratio()
        heap_limit = numerator * 1048576 // denominator
    return {'window': 'post-recovery soak', 'limits': limits,
            'heap': {'metric': 'usedHeapBytes', 'unit': 'bytes',
                     'limit': heap_limit,
                     'status': 'not_run', 'failures': 0,
                     'nodes': {node: {'maximum': None, 'samples': 0} for node in nodes}},
            'threads': {'metric': 'totalThreads', 'unit': 'threads', 'limit': max_threads,
                        'status': 'not_run', 'failures': 0,
                        'nodes': {node: {'maximum': None, 'samples': 0} for node in nodes}}}


def observe_resource_budgets(budgets, resources):
    for name in ('heap', 'threads'):
        budget = budgets[name]
        errors = []
        for node, observed in budget['nodes'].items():
            snapshot = resources.get(node)
            value = snapshot.get(budget['metric']) if isinstance(snapshot, dict) else None
            valid = (isinstance(value, (int, float)) and not isinstance(value, bool)
                     and math.isfinite(value) and value >= 0 and int(value) == value)
            if name == 'threads' and valid:
                valid = value >= 1
            if not valid:
                errors.append(node + ': missing or invalid ' + budget['metric'])
                continue
            observed['samples'] += 1
            observed['maximum'] = value if observed['maximum'] is None else max(observed['maximum'], value)
            if budget['limit'] is not None and value > budget['limit']:
                errors.append(f'{node}: {budget["metric"]}={value} exceeds {budget["limit"]}')
        if budget['limit'] is None:
            budget['status'] = 'not_configured'
        elif errors:
            budget['status'] = 'failed'
            budget['failures'] += 1
            budget['lastError'] = '; '.join(errors)
        elif budget['status'] != 'failed':
            budget['status'] = 'passed'
    return all(budgets[name]['status'] != 'failed' for name in ('heap', 'threads'))


def validate_indexed_records(records, tag_indices, start_tick, end_tick):
    assert end_tick - start_tick >= 3, 'Content capture needs at least two interior ticks'
    assert records, 'No controlled content records'
    assert len(records) < CONTENT_RECORD_LIMIT, 'Controlled content reached the record cap'
    expected = set(tag_indices)
    previous = {}
    members = {tick: set() for tick in range(start_tick + 1, end_tick)}
    for record in records:
        alias = record.get('tag')
        match = re.fullmatch(r'Tag(\d+)', alias) if isinstance(alias, str) else None
        assert match and int(match[1]) in expected, 'Unexpected controlled tag alias: ' + str(alias)
        index = int(match[1])
        assert alias == 'Tag' + str(index), 'Noncanonical controlled tag alias: ' + alias
        node_id = f'ns=2;s=Load.Tag{index}'
        assert record.get('nodeId') == node_id, 'Wrong NodeId for ' + alias
        assert 'configuredNodeId' not in record, 'Unexpected configured NodeId for ' + alias
        assert record.get('quality') == 'Good', 'Bad controlled quality for ' + alias
        value = record.get('value')
        assert (isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(value) and value >= 0 and int(value) == value), 'Invalid indexed value for ' + alias
        tick, value_index = divmod(int(value), INDEXED_VALUE_STRIDE)
        assert value_index == index, 'Wrong value index for ' + alias
        assert tick > previous.get(index, -1), 'Duplicate or regressing tick for ' + alias
        previous[index] = tick
        if tick in members:
            members[tick].add(index)
    for tick, seen in members.items():
        assert seen == expected, f'Incomplete interior tick {tick}: expected {sorted(expected)}, received {sorted(seen)}'
    return {'mode': 'indexed', 'stride': INDEXED_VALUE_STRIDE, 'sampleRecords': len(records),
            'tags': len(expected), 'captureStartTick': start_tick, 'captureEndTick': end_tick,
            'firstInteriorTick': start_tick + 1, 'lastInteriorTick': end_tick - 1,
            'interiorTicks': len(members)}


class Harness:
    def __init__(self, args):
        self.args = args
        self.name = 'opcua-acceptance-' + secrets.token_hex(4)
        self.out = args.output.resolve()
        if ROOT / 'dist' not in self.out.parents:
            raise ValueError('Acceptance output must be inside ignored dist/')
        self.out.mkdir(parents=True, exist_ok=False)
        self.security = self.out / 'security'
        self.security.mkdir()
        self.containers = []
        self.network_attempted = False
        self.ports = {}
        self.progress_uuids = set()
        self.nodes = ['node1', 'node2'] if args.profile == 'nifi1' else ['node1']
        self.report = {'startedAt': datetime.now(timezone.utc).isoformat(), 'profile': args.profile,
                       'tags': args.tags, 'soakSeconds': args.seconds, 'checks': [], 'samples': [],
                       'valueMode': 'indexed', 'valueStride': INDEXED_VALUE_STRIDE,
                       'resourceBudgets': resource_budget_report(self.nodes, getattr(args, 'max_heap_mib', None),
                                                                getattr(args, 'max_threads', None)),
                       'narSha256': hashlib.sha256(args.nar.read_bytes()).hexdigest(), 'passed': False}
        self.report.update(harness_provenance())
        if getattr(args, 'etl_only', False):
            self.report['scope'] = 'Default delivery and ETL output/configuration checks; no recovery or soak'
            self.report['soakSeconds'] = 0

    def save(self):
        (self.out / 'report.json').write_text(json.dumps(self.report, indent=2) + '\n')

    def check(self, name, detail=None):
        self.report['checks'].append({'name': name, 'detail': detail})
        self.save()
        print(name, flush=True)

    def run(self, suffix, image, options=(), entry=()):
        name = self.name + '-' + suffix
        self.containers.append(name)
        command('docker', 'run', '-d', '--name', name, '--hostname', suffix,
                '--network', self.name, '--network-alias', suffix,
                '--label', 'opcua.acceptance=' + self.name, *options, image, *entry)
        return name

    def api(self, node, method, path, body=None, timeout=20):
        req = urllib.request.Request('http://127.0.0.1:' + self.ports[node] + '/nifi-api' + path,
              method=method, data=None if body is None else json.dumps(body).encode(),
              headers={'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                data = res.read()
                return json.loads(data) if data else None
        except urllib.error.HTTPError as e:
            raise RuntimeError(f'{method} {path}: {e.code} {e.read().decode()[:1500]}') from None

    def wait(self, description, fn, timeout=180):
        started = time.monotonic()
        end = started + timeout
        next_update = started
        last = None
        while time.monotonic() < end:
            try:
                result = fn()
                if result:
                    return result
                last = repr(result)
            except (OSError, ValueError, RuntimeError) as e:
                last = str(e)
            now = time.monotonic()
            if now >= next_update:
                detail = {'phase': description, 'elapsedSeconds': round(now - started, 1),
                          'limitSeconds': timeout, 'lastProgress': last}
                self.report['progress'] = detail
                self.save()
                print('Waiting: ' + json.dumps(detail), flush=True)
                next_update = now + 15
            time.sleep(min(2, max(0, end - time.monotonic())))
        elapsed = time.monotonic() - started
        self.report['progress'] = {'phase': description, 'elapsedSeconds': round(elapsed, 1),
                                   'limitSeconds': timeout, 'lastProgress': last}
        self.save()
        raise RuntimeError(f'Timed out: {description}; elapsed={elapsed:.1f}s; '
                           f'limit={timeout}s; last progress={last}')

    def metrics(self):
        return json.loads((self.security / 'metrics.json').read_text())

    def setup(self):
        self.network_attempted = True
        command('docker', 'network', 'create', '--subnet', self.args.subnet,
                '--label', 'opcua.acceptance=' + self.name, self.name)
        self.server = self.run('opcua', 'maven:3.9.11-eclipse-temurin-8',
             ['--memory', '1g', '--cpus', '2',
              '--user', str(__import__('os').getuid()) + ':' + str(__import__('os').getgid()),
              '-e', 'MAVEN_CONFIG=/tmp/maven', '-e', 'JAVA_TOOL_OPTIONS=-XX:ActiveProcessorCount=4',
              '-e', 'DEMO_LOAD_TAGS=' + str(self.args.tags),
              '-e', 'DEMO_LOAD_VALUE_MODE=indexed',
              '-v', str(self.args.demo_target.resolve()) + ':/app:ro', '-v', str(self.security) + ':/security'],
             ['java', '-cp', '/app/classes:/app/lib/*', 'io.github.paf91.nifi.opcua.demo.DemoServer', '/security'])
        self.wait('OPC server', lambda: (self.security / 'metrics.json').exists())
        cluster = self.args.profile == 'nifi1'
        if cluster:
            self.run('zk', 'zookeeper:3.9.4', ['--memory', '512m', '--cpus', '2', '-e', 'JVMFLAGS=-Xmx256m'])
        key = secrets.token_urlsafe(32)
        self.nodes = ['node1', 'node2'] if cluster else ['node1']
        self.names = {}
        for node in self.nodes:
            image = 'opcua-acceptance-nifi1:local' if cluster else 'apache/nifi:2.12.0'
            self.names[node] = self.run(node, image,
                ['--memory', '3g', '--cpus', '2', '-e', 'JAVA_TOOL_OPTIONS=-XX:ActiveProcessorCount=4',
                 '-e', 'NIFI_SENSITIVE_PROPS_KEY=' + key, '-e', 'ACCEPTANCE_CLUSTER=' + str(cluster).lower(),
                 '-p', '127.0.0.1::8080', '-v', str(self.security) + ':/security',
                 '-v', str(self.args.nar.resolve()) + ':/opt/nifi/nifi-current/lib/opcua-acceptance.nar:ro',
                 '-v', str(ROOT / 'acceptance/start-nifi.sh') + ':/acceptance/start.sh:ro',
                 '--entrypoint', '/bin/sh'], ['/acceptance/start.sh'])
            self.ports[node] = command('docker', 'port', self.names[node], '8080/tcp').rsplit(':', 1)[1]
        for node in self.nodes:
            self.wait(node + ' REST API', lambda n=node: self.api(n, 'GET', '/flow/processor-types'), 420)
        self.node = 'node1'
        if cluster:
            self.wait('two connected cluster nodes', lambda: len(self.connected()) == 2)
        self.report['runtimes'] = {node: {'imageId': json.loads(command('docker', 'inspect', self.names[node]))[0]['Image'], 'javaVersion': command('docker', 'exec', self.names[node], 'java', '-version')} for node in self.nodes}
        self.check('runtime-started', {'nodes': len(self.nodes)})

    def connected(self):
        return [n for n in self.api(self.node, 'GET', '/controller/cluster')['cluster']['nodes']
                if n['status'] == 'CONNECTED']

    def state(self, processor, state):
        entity = self.api(self.node, 'GET', '/processors/' + processor)
        self.api(self.node, 'PUT', '/processors/' + processor + '/run-status',
                 {'revision': entity['revision'], 'state': state, 'disconnectedNodeAcknowledged': True})

    def flow(self):
        self.report['processorType'] = 'ConsumeOPCUA'
        root = self.api(self.node, 'GET', '/flow/process-groups/root')['processGroupFlow']['id']
        group = self.api(self.node, 'POST', '/process-groups/' + root + '/process-groups',
                    {'revision': {'version': 0}, 'component': {'name': 'Synthetic acceptance', 'position': {'x': 0, 'y': 0}}})
        self.gid = group['id']
        types = self.api(self.node, 'GET', '/flow/processor-types')['processorTypes']
        def add(kind, config):
            if kind == 'ConsumeOPCUA':
                t = next(t for t in types if t['type'] == 'io.github.paf91.nifi.opcua.ConsumeOPCUA'
                         and t['bundle']['group'] == 'io.github.paf91.nifi'
                         and t['bundle']['artifact'] == 'nifi-opcua-nar')
            else:
                t = next(t for t in types if t['type'].endswith('.' + kind))
            return self.api(self.node, 'POST', '/process-groups/' + self.gid + '/processors',
                {'revision': {'version': 0}, 'component': {'type': t['type'], 'bundle': t['bundle'],
                 'name': kind, 'position': {'x': 0, 'y': 0}, 'config': config}})['id']
        self.source = add('ConsumeOPCUA', {'schedulingPeriod': '100 ms',
            'properties': {'Endpoint URL': 'opc.tcp://opcua:4840/demo',
                'Client Certificate File': '/security/client.der', 'Client Private Key File': '/security/client-key.der',
                'PKI Directory': '/security/nifi-pki', 'Username': 'demo', 'Password': 'demo-password',
                'Tags': '\n'.join(f'Tag{i} | ns=2;s=Load.Tag{i}' for i in range(self.args.tags)),
                'Default Sampling Interval (ms)': '1000', 'Notification Buffer Capacity': '5000',
                'Max Records Per FlowFile': '1000', 'Request Timeout (ms)': '5000', 'Reconnect Delay (ms)': '1000'}})
        self.sink = add('LogAttribute', {'autoTerminatedRelationships': ['success'],
                                       'properties': {'Log Payload': 'false'}})
        conn = self.api(self.node, 'POST', '/process-groups/' + self.gid + '/connections',
            {'revision': {'version': 0}, 'component': {'name': 'acceptance',
             'source': {'id': self.source, 'groupId': self.gid, 'type': 'PROCESSOR'},
             'destination': {'id': self.sink, 'groupId': self.gid, 'type': 'PROCESSOR'},
             'selectedRelationships': ['success'], 'backPressureObjectThreshold': 10,
             'backPressureDataSizeThreshold': '10 MB'}})
        self.connection = conn['id']
        for processor in [self.source, self.sink]:
            self.wait('valid processor', lambda p=processor: self.api(self.node, 'GET', '/processors/' + p)['component']['validationStatus'] == 'VALID')
        self.state(self.source, 'RUNNING')
        self.wait('queued notifications', lambda: self.queued() > 0)
        self.inspect_content()
        self.state(self.sink, 'RUNNING')
        self.wait('one active subscription', lambda: self.metrics()['subscriptions'] == 1 and self.metrics()['monitoredItems'] == self.args.tags)
        self.check('secure-delivery', self.metrics())

    def queued(self):
        status = self.api(self.node, 'GET', '/flow/connections/' + self.connection + '/status')['connectionStatus']
        return status['aggregateSnapshot']['flowFilesQueued']

    def inspect_content(self, record_check=True):
        base = '/flowfile-queues/' + self.connection
        listing = self.api(self.node, 'POST', base + '/listing-requests')['listingRequest']
        lid = listing['id']
        try:
            listing = self.wait('queue listing', lambda: self._listing(base, lid))
            rows = listing['flowFileSummaries']
            assert rows, 'No queued content to inspect'
            row = rows[0]
            records = self.read_content(base, row)
            assert records and all(r['quality'] == 'Good' and r['nodeId'].startswith('ns=2;s=Load.Tag') for r in records)
        finally:
            self.api(self.node, 'DELETE', base + '/listing-requests/' + lid)
        freshness = validate_fresh_records(records)
        detail = {'sampleRecords': len(records), 'fields': sorted(records[0]),
                  'flowFileUuid': row['uuid'], **freshness}
        if record_check:
            self.check('json-content-validated', detail)
        return detail

    def read_content(self, base, row, timeout=20):
        query = ('?clusterNodeId=' + row['clusterNodeId']) if row.get('clusterNodeId') else ''
        request = urllib.request.Request('http://127.0.0.1:' + self.ports[self.node] + '/nifi-api'
                  + base + '/flowfiles/' + row['uuid'] + '/content' + query)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            content = response.read(CONTENT_BYTE_LIMIT + 1)
        assert len(content) < CONTENT_BYTE_LIMIT, 'Content reached the byte cap'
        records = json.loads(content)
        assert isinstance(records, list), 'Expected a JSON record array'
        return records

    def capture_content(self):
        started = time.monotonic()
        end = started + CONTENT_CAPTURE_SECONDS
        base = '/flowfile-queues/' + self.connection
        listing = self.api(self.node, 'POST', base + '/listing-requests',
                           timeout=min(20, CONTENT_CAPTURE_SECONDS))['listingRequest']
        lid = listing['id']
        try:
            listing = self.wait('controlled content queue listing',
                                lambda: self._listing(base, lid, timeout=min(20, max(0.1, end - time.monotonic()))),
                                max(1, end - time.monotonic()))
            rows = listing['flowFileSummaries']
            assert rows and len(rows) < CONTENT_FLOWFILE_LIMIT, 'Controlled content reached the FlowFile cap or is empty'
            assert listing.get('queueSize', {}).get('objectCount') == len(rows), 'Truncated controlled content listing'
            positions = [row.get('position') for row in rows]
            assert all(isinstance(position, int) for position in positions), 'Missing queue position'
            assert len(set(positions)) == len(rows), 'Duplicate queue position'
            records = []
            for row in sorted(rows, key=lambda row: row['position']):
                remaining = end - time.monotonic()
                assert remaining > 0, (f'Controlled content capture timed out; elapsed={time.monotonic() - started:.1f}s; '
                                       f'limit={CONTENT_CAPTURE_SECONDS}s; last progress={len(records)} records')
                records.extend(self.read_content(base, row, timeout=min(20, remaining)))
                assert len(records) < CONTENT_RECORD_LIMIT, 'Controlled content reached the record cap'
            assert time.monotonic() < end, 'Controlled content capture exceeded its time limit'
            return records, len(rows)
        finally:
            self.api(self.node, 'DELETE', base + '/listing-requests/' + lid)

    def source_properties(self, properties):
        entity = self.api(self.node, 'GET', '/processors/' + self.source)
        self.api(self.node, 'PUT', '/processors/' + self.source,
                 {'revision': entity['revision'], 'component': {'id': self.source,
                  'config': {'properties': properties}}})

    def connection_settings(self, settings):
        entity = self.api(self.node, 'GET', '/connections/' + self.connection)
        self.api(self.node, 'PUT', '/connections/' + self.connection,
                 {'revision': entity['revision'], 'component': {'id': self.connection, **settings}})

    def controlled_content(self):
        count = min(self.args.tags, CONTENT_TAG_LIMIT)
        properties = {'Tags': '\n'.join(f'Tag{i} | ns=2;s=Load.Tag{i}' for i in range(count)),
                      'Default Sampling Interval (ms)': '100', 'Publishing Interval (ms)': '100',
                      'Default Monitored Item Queue Size': '32', 'Notification Buffer Capacity': '4096',
                      'Max Records Per FlowFile': '128'}
        original = self.api(self.node, 'GET', '/processors/' + self.source)['component']['config']['properties']
        original = {key: original.get(key) for key in properties}
        connection = self.api(self.node, 'GET', '/connections/' + self.connection)['component']
        original_connection = {key: connection[key] for key in ('backPressureObjectThreshold', 'backPressureDataSizeThreshold')}
        primary_error = None
        try:
            self.state(self.source, 'STOPPED')
            self.wait('source stopped before controlled content', lambda: self.stopped(self.source))
            self.wait('subscription closed before controlled content', lambda: self.metrics()['subscriptions'] == 0)
            self.wait('old output drained before controlled content', lambda: self.queue_count_now() == 0)
            self.state(self.sink, 'STOPPED')
            self.wait('sink stopped for controlled content', lambda: self.stopped(self.sink))
            self.source_properties(properties)
            self.connection_settings({'backPressureObjectThreshold': CONTENT_FLOWFILE_LIMIT,
                                      'backPressureDataSizeThreshold': '20 MB'})
            self.wait('controlled content settings valid', lambda: self.api(self.node, 'GET', '/processors/' + self.source)
                      ['component']['validationStatus'] == 'VALID')
            self.state(self.source, 'RUNNING')
            self.wait('controlled content subscription', lambda: self.metrics()['subscriptions'] == 1
                      and self.metrics()['monitoredItems'] == count)
            start_tick = self.metrics()['tick']
            self.wait('controlled content tick window', lambda: self.metrics()['tick'] >= start_tick + 4, 15)
            end_tick = self.metrics()['tick']
            self.state(self.source, 'STOPPED')
            self.wait('controlled content source stopped', lambda: self.stopped(self.source), 15)
            records, flowfiles = self.capture_content()
            detail = validate_indexed_records(records, range(count), start_tick, end_tick)
            detail.update({'flowFiles': flowfiles, 'samplingIntervalMs': 100, 'publishingIntervalMs': 100,
                           'flowFileCap': CONTENT_FLOWFILE_LIMIT, 'recordCap': CONTENT_RECORD_LIMIT,
                           'captureLimitSeconds': CONTENT_CAPTURE_SECONDS})
            self.check('controlled-content-sequence', detail)
        except BaseException as e:
            primary_error = e
            raise
        finally:
            try:
                self.state(self.source, 'STOPPED')
                self.wait('source stopped before restoring full load', lambda: self.stopped(self.source), 15)
                self.state(self.sink, 'RUNNING')
                self.wait('controlled content drained', lambda: self.queue_count_now() == 0)
                self.source_properties(original)
                self.connection_settings(original_connection)
                self.wait('full load settings restored', lambda: self.api(self.node, 'GET', '/processors/' + self.source)
                          ['component']['validationStatus'] == 'VALID')
                self.state(self.source, 'RUNNING')
                self.wait('full load subscription restored', lambda: self.metrics()['subscriptions'] == 1
                          and self.metrics()['monitoredItems'] == self.args.tags)
            except Exception as e:
                self.report['contentRestoreError'] = str(e)
                self.save()
                if primary_error is None:
                    raise

    def _listing(self, base, lid, timeout=20):
        result = self.api(self.node, 'GET', base + '/listing-requests/' + lid,
                          timeout=timeout)['listingRequest']
        return result if result['finished'] else None

    def etl_output(self):
        """Exercise the opt-in output contract through real NiFi property/relationship APIs."""
        self.state(self.source, 'STOPPED')
        self.wait('source stopped before ETL settings', lambda: self.stopped(self.source))
        self.wait('old output drained', lambda: self.queue_count_now() == 0)
        self.state(self.sink, 'STOPPED')
        self.wait('sink stopped for ETL inspection', lambda: self.stopped(self.sink))
        fields = 'tag,value,statusCode,sourceTimestamp,serverTimestamp,dataType,engineeringUnits'
        entity = self.api(self.node, 'GET', '/processors/' + self.source)
        self.api(self.node, 'PUT', '/processors/' + self.source,
                 {'revision': entity['revision'], 'component': {'id': self.source, 'config': {'properties': {
                     'Output Fields': fields, 'Output Format': 'NDJSON',
                     'Timestamp Format': 'Epoch Milliseconds', 'Route by Quality': 'true',
                     'Maximum Batch Delay (ms)': '250'}}}})
        extra_connections = []
        for relationship in ('bad', 'uncertain'):
            connection = self.api(self.node, 'POST', '/process-groups/' + self.gid + '/connections',
                {'revision': {'version': 0}, 'component': {'name': 'ETL ' + relationship,
                 'source': {'id': self.source, 'groupId': self.gid, 'type': 'PROCESSOR'},
                 'destination': {'id': self.sink, 'groupId': self.gid, 'type': 'PROCESSOR'},
                 'selectedRelationships': [relationship], 'backPressureObjectThreshold': 10,
                 'backPressureDataSizeThreshold': '10 MB'}})
            extra_connections.append(connection['id'])
        self.wait('ETL settings valid', lambda: self.api(self.node, 'GET', '/processors/' + self.source)['component']['validationStatus'] == 'VALID')
        self.state(self.source, 'RUNNING')
        self.wait('ETL records queued', lambda: self.queue_count_now() > 0)
        base = '/flowfile-queues/' + self.connection
        listing = self.api(self.node, 'POST', base + '/listing-requests')['listingRequest']
        lid = listing['id']
        try:
            rows = self.wait('ETL queue listing', lambda: self._listing(base, lid))['flowFileSummaries']
            assert rows, 'No ETL output'
            row = rows[0]
            query = ('?clusterNodeId=' + row['clusterNodeId']) if row.get('clusterNodeId') else ''
            path = base + '/flowfiles/' + row['uuid']
            detail = self.api(self.node, 'GET', path + query)['flowFile']
            url = 'http://127.0.0.1:' + self.ports[self.node] + '/nifi-api' + path + '/content' + query
            with urllib.request.urlopen(url, timeout=20) as response:
                content = response.read()
            records = [json.loads(line) for line in content.splitlines()]
            assert content.endswith(b'\n') and records
            assert all(set(record) == set(fields.split(',')) for record in records)
            assert all(record['statusCode'] == '0x00000000' for record in records)
            assert all(record['dataType'] in ('i=11', 'ns=0;i=11') for record in records)
            assert all(record['engineeringUnits'] is None for record in records), 'Demo has no unit property'
            for field in ('sourceTimestamp', 'serverTimestamp'):
                assert all(isinstance(record[field], int) and abs(time.time() * 1000 - record[field]) < 30000 for record in records)
            attributes = detail['attributes']
            assert attributes['mime.type'] == 'application/x-ndjson'
            assert attributes['opcua.output.format'] == 'NDJSON'
            assert attributes['opcua.timestamp.format'] == 'Epoch Milliseconds'
            assert attributes['opcua.quality.route'] == 'success'
            assert int(attributes['record.count']) == len(records)
            self.check('etl-ndjson-projection-timestamps-metadata', {'sampleRecords': len(records), 'fields': sorted(records[0]),
                       'qualityRelationshipsConnected': ['bad', 'uncertain']})
        finally:
            self.api(self.node, 'DELETE', base + '/listing-requests/' + lid)
        self.state(self.source, 'STOPPED')
        self.wait('ETL source stopped', lambda: self.stopped(self.source))
        self.state(self.sink, 'RUNNING')
        self.wait('ETL output drained', lambda: self.queue_count_now() == 0)
        self.state(self.sink, 'STOPPED')
        self.wait('sink stopped before restoring output', lambda: self.stopped(self.sink))
        for connection in extra_connections:
            entity = self.api(self.node, 'GET', '/connections/' + connection)
            self.api(self.node, 'DELETE', '/connections/' + connection + '?version=' + str(entity['revision']['version']))
        entity = self.api(self.node, 'GET', '/processors/' + self.source)
        self.api(self.node, 'PUT', '/processors/' + self.source,
                 {'revision': entity['revision'], 'component': {'id': self.source, 'config': {'properties': {
                     'Output Fields': 'tag,nodeId,configuredNodeId,namespaceUri,value,valueType,statusCode,quality,sourceTimestamp,receivedTimestamp',
                     'Output Format': 'JSON Array', 'Timestamp Format': 'ISO-8601',
                     'Route by Quality': 'false', 'Maximum Batch Delay (ms)': '0'}}}})
        self.wait('restored output valid', lambda: self.api(self.node, 'GET', '/processors/' + self.source)['component']['validationStatus'] == 'VALID')
        self.state(self.source, 'RUNNING')
        self.wait('restored JSON output queued', lambda: self.queue_count_now() > 0)
        self.inspect_content()
        self.state(self.sink, 'RUNNING')
        self.check('etl-default-output-restored')

    def snapshot(self, phase):
        status = self.api(self.node, 'GET', '/flow/processors/' + self.source + '/status?nodewise=true')['processorStatus']
        diagnostics = self.api(self.node, 'GET', '/system-diagnostics?nodewise=true')['systemDiagnostics']
        resources = resource_snapshots(diagnostics, self.node)
        sample = {'at': time.time(), 'phase': phase, 'server': self.metrics(), 'resources': resources,
                  'processor': status, 'counters': self.api(self.node, 'GET', '/counters')}
        self.report['samples'].append(sample)
        del self.report['samples'][:-SAMPLE_HISTORY_LIMIT]
        self.save()
        return sample

    def stopped(self, processor):
        entity = self.api(self.node, 'GET', '/processors/' + processor)
        active = entity['status']['aggregateSnapshot'].get('activeThreadCount')
        return (entity['component']['state'] == 'STOPPED'
                and type(active) is int and active == 0)

    def stopped_resources(self):
        stopped = self.stopped(self.source)
        server = self.metrics()
        counts = [server.get(name) for name in ('sessions', 'subscriptions', 'monitoredItems')]
        if stopped and all(type(value) is int and value == 0 for value in counts):
            return {'processorStopped': True, 'server': server}
        return False

    def final_stop(self):
        self.state(self.source, 'STOPPED')
        observed = self.wait('final stop cleanup', self.stopped_resources, timeout=30)
        self.snapshot('stopped')
        self.check('final-stop-cleanup', observed)

    def configure_source_period(self, period):
        entity = self.api(self.node, 'GET', '/processors/' + self.source)
        self.api(self.node, 'PUT', '/processors/' + self.source,
                 {'revision': entity['revision'], 'component': {
                     'id': self.source, 'config': {'schedulingPeriod': period,
                         'properties': {'Stop Drain Timeout (ms)': '5000'}}}})

    def queue_count_now(self):
        # A fresh listing avoids the periodic status cache when comparing stop-time delivery.
        base = '/flowfile-queues/' + self.connection
        request = self.api(self.node, 'POST', base + '/listing-requests')['listingRequest']
        lid = request['id']
        try:
            listing = self.wait('fresh stop-test queue listing', lambda: self._listing(base, lid))
            rows = listing['flowFileSummaries']
            assert len(rows) < 100, 'Stop test must not compare truncated listings'
            return len(rows)
        finally:
            self.api(self.node, 'DELETE', base + '/listing-requests/' + lid)

    def stop_warnings(self):
        board = self.api(self.node, 'GET', '/flow/bulletin-board?sourceId=' + self.source)
        result = {}
        for entry in board['bulletinBoard']['bulletins']:
            bulletin = entry['bulletin']
            if (bulletin.get('sourceId') == self.source
                    and 'Stopped with ' in bulletin.get('message', '')
                    and 'uncommitted notifications' in bulletin['message']):
                key = (bulletin.get('nodeAddress'), bulletin['id'])
                result[key] = bulletin['message']
        return result

    def graceful_stop(self):
        self.state(self.source, 'STOPPED')
        self.wait('source stopped before drain test', lambda: self.stopped(self.source), 15)
        self.wait('old subscription closed', lambda: self.metrics()['subscriptions'] == 0)
        self.wait('old output drained', lambda: self.queued() == 0)
        self.state(self.sink, 'STOPPED')
        self.wait('sink stopped before drain test', lambda: self.stopped(self.sink), 15)
        self.configure_source_period('30 sec')
        prior_warnings = self.stop_warnings()
        started = time.monotonic()
        self.state(self.source, 'RUNNING')
        self.wait('subscription ready for buffered stop', lambda: self.metrics()['subscriptions'] == 1)
        self.wait('first trigger returned', lambda: self.api(self.node, 'GET', '/processors/' + self.source)
                  ['status']['aggregateSnapshot']['activeThreadCount'] == 0)
        time.sleep(2.5)  # Receive updates without another scheduled trigger draining the buffer.
        before = self.queue_count_now()
        assert time.monotonic() - started < 20, 'Setup took too long to isolate the final drain'
        stopped_at = time.monotonic()
        self.state(self.source, 'STOPPED')
        self.wait('bounded graceful stop', lambda: self.stopped(self.source), 15)
        duration = time.monotonic() - stopped_at
        self.wait('subscription closed before drained stop', lambda: self.metrics()['subscriptions'] == 0)
        after = self.queue_count_now()
        assert after > before, 'Stop did not commit buffered notifications between scheduled triggers'
        warnings = self.stop_warnings()
        assert not set(warnings) - set(prior_warnings), 'Healthy stop emitted an uncommitted-data warning'
        self.check('graceful-stop-drained', {'seconds': duration, 'queuedBefore': before, 'queuedAfter': after})

        self.configure_source_period('100 ms')
        self.state(self.source, 'RUNNING')
        self.wait('backpressure before stop', lambda: self.queued() >= 10)
        time.sleep(2.5)
        prior_warnings = self.stop_warnings()
        stopped_at = time.monotonic()
        self.state(self.source, 'STOPPED')
        self.wait('stop despite backpressure', lambda: self.stopped(self.source), 15)
        duration = time.monotonic() - stopped_at
        self.wait('subscription closed under backpressure', lambda: self.metrics()['subscriptions'] == 0)
        def retained_warning():
            warnings = self.stop_warnings()
            return any('backpressured' in message for key, message in warnings.items() if key not in prior_warnings)
        self.wait('retained-data warning after blocked drain', retained_warning, 15)
        self.check('backpressured-stop-bounded', {'seconds': duration})
        self.state(self.sink, 'RUNNING')
        self.wait('output available after blocked drain', lambda: self.queued() == 0)
        self.state(self.source, 'RUNNING')
        self.wait('subscription restored after retained drain', lambda: self.metrics()['subscriptions'] == 1)
        self.delivery_progress('delivery-after-stop-drain')

    def exercise(self):
        self.controlled_content()
        self.graceful_stop()
        self.snapshot('baseline')
        self.state(self.sink, 'STOPPED')
        self.wait('backpressure reached', lambda: self.queued() >= 10)
        time.sleep(10)
        self.snapshot('backpressure')
        self.state(self.sink, 'RUNNING')
        self.wait('queue drained', lambda: self.queued() < 10)
        self.check('backpressure-recovery')
        self.state(self.source, 'STOPPED')
        self.wait('subscription closed after stop', lambda: self.metrics()['subscriptions'] == 0)
        self.state(self.source, 'RUNNING')
        self.wait('subscription restored after start', lambda: self.metrics()['subscriptions'] == 1)
        self.check('stop-start-recovery')
        command('docker', 'stop', '-t', '10', self.server)
        time.sleep(10)
        (self.security / 'metrics.json').unlink(missing_ok=True)
        command('docker', 'start', self.server)
        self.wait('server restart recovery', lambda: self.metrics()['subscriptions'] == 1, 120)
        self.delivery_progress('server-restart-delivery')
        self.check('server-restart-recovery')
        command('docker', 'pause', self.server)
        try:
            time.sleep(15)
        finally:
            command('docker', 'unpause', self.server)
        self.delivery_progress('network-stall-recovery')
        if len(self.nodes) == 2:
            self.failover()
        self.soak()
        self.final_stop()

    def soak(self):
        started = time.monotonic()
        end = started + self.args.seconds
        progress = {'checks': 0, 'intervalSeconds': SOAK_PROGRESS_SECONDS}
        self.report['soakProgress'] = progress
        while time.monotonic() < end:
            next_check = time.monotonic() + SOAK_PROGRESS_SECONDS
            sample = self.snapshot('soak')
            assert sample['server']['subscriptions'] == 1, 'Unexpected subscription count during steady load'
            passed = observe_resource_budgets(self.report['resourceBudgets'], sample['resources'])
            self.save()
            failures = [self.report['resourceBudgets'][name].get('lastError', '')
                        for name in ('heap', 'threads') if self.report['resourceBudgets'][name]['status'] == 'failed']
            assert passed, 'Soak resource budget failed: ' + '; '.join(failures)
            detail = self.delivery_progress('soak fresh delivery', record_check=False)
            progress.update({'checks': progress['checks'] + 1, 'lastAt': time.time(),
                             'lastFlowFileUuid': detail['flowFileUuid'],
                             'newestSourceTimestamp': detail['newestSourceTimestamp']})
            self.save()
            elapsed = time.monotonic() - started
            print(f'Soak: elapsed={elapsed:.1f}s; limit={self.args.seconds}s; '
                  f'last progress={progress["checks"]} fresh deliveries', flush=True)
            time.sleep(max(0, min(next_check, end) - time.monotonic()))
        self.check('soak-completed', {'seconds': self.args.seconds,
                                    'elapsedSeconds': round(time.monotonic() - started, 1),
                                    'freshDeliveryChecks': progress['checks']})

    def delivery_progress(self, name, record_check=True):
        self.wait('drain before freshness check', lambda: self.queue_count_now() == 0, 120)
        self.state(self.sink, 'STOPPED')
        try:
            self.wait('sink stopped for freshness check', lambda: self.stopped(self.sink), 30)
            self.wait(name, lambda: self.queue_count_now() > 0, 30)
            detail = self.inspect_content(record_check=record_check)
            assert detail['flowFileUuid'] not in self.progress_uuids, 'Repeated FlowFile does not prove delivery progress'
            self.progress_uuids = {detail['flowFileUuid']}
        finally:
            self.state(self.sink, 'RUNNING')
        if record_check:
            self.check(name, detail)
        return detail

    def primary_ownership(self):
        primary = next(n['address'] for n in self.connected() if 'Primary Node' in n.get('roles', []))
        before = self.snapshot('ownership-before')['processor']['nodeSnapshots']
        time.sleep(5)
        after = self.snapshot('ownership-after')['processor']['nodeSnapshots']
        prior = {n['address']: n['statusSnapshot']['flowFilesOut'] for n in before}
        delta = {n['address']: n['statusSnapshot']['flowFilesOut'] - prior[n['address']] for n in after}
        assert delta[primary] > 0, 'Primary did not emit data'
        assert all(v == 0 for k, v in delta.items() if k != primary), 'Non-primary emitted data'
        assert self.metrics()['subscriptions'] == 1, 'More than one subscription'
        self.check('primary-only-delivery', {'primary': primary, 'flowFileDelta': delta})

    def failover(self):
        self.primary_ownership()
        nodes = self.connected()
        primary = next(n for n in nodes if 'Primary Node' in n.get('roles', []))
        old = primary['address']
        survivor = next(n for n in self.nodes if n != old)
        self.snapshot('before-failover')
        command('docker', 'stop', '-t', '30', self.names[old])
        self.node = survivor
        self.wait('primary transferred', lambda: any(n['address'] == survivor and 'Primary Node' in n.get('roles', []) for n in self.connected()), 150)
        self.wait('subscription on new primary', lambda: self.metrics()['subscriptions'] == 1)
        self.delivery_progress('new-primary-delivery')
        self.snapshot('after-failover')
        command('docker', 'start', self.names[old])
        self.ports[old] = command('docker', 'port', self.names[old], '8080/tcp').rsplit(':', 1)[1]
        self.wait('old primary rejoined', lambda: len(self.connected()) == 2, 300)
        self.wait('single subscription after rejoin', lambda: self.metrics()['subscriptions'] == 1)
        self.primary_ownership()
        self.check('primary-failover-and-rejoin', {'oldPrimary': old, 'newPrimary': survivor})

    def owned_resource_id(self, name, network=False):
        arguments = ('network', 'ls') if network else ('ps', '--all')
        output = command('docker', *arguments, '--no-trunc',
                         '--filter', 'label=opcua.acceptance=' + self.name,
                         '--format', '{{.ID}} {{.Name}}' if network else '{{.ID}} {{.Names}}')
        matches = []
        for line in output.splitlines():
            resource_id, resource_name = line.split(maxsplit=1)
            if resource_name == name:
                matches.append(resource_id)
        if len(matches) > 1:
            raise RuntimeError('Multiple owned resources named ' + name)
        return matches[0] if matches else None

    def cleanup(self):
        errors = []
        def record_error(action, name, error):
            detail = {'action': action, 'resource': name, 'error': str(error)}
            errors.append(detail)
            print('Cleanup: ' + json.dumps(detail), flush=True)
        for name in reversed(self.containers):
            try:
                resource_id = self.owned_resource_id(name)
            except Exception as e:
                record_error('query-container', name, e)
                continue
            if resource_id is None:
                continue
            suffix = name.rsplit('-', 1)[1]
            try:
                (self.out / (suffix + '.log')).write_text(command('docker', 'logs', resource_id))
            except Exception as e:
                record_error('logs', name, e)
            if suffix.startswith('node'):
                try:
                    subprocess.run(['docker', 'cp', resource_id + ':/opt/nifi/nifi-current/logs/nifi-app.log',
                                    str(self.out / (suffix + '-app.log'))], check=True,
                                   timeout=CLEANUP_TIMEOUT_SECONDS, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception as e:
                    record_error('copy-log', name, e)
            try:
                command('docker', 'rm', '-f', '-v', resource_id)
            except Exception as e:
                record_error('remove-container', name, e)
        if self.network_attempted:
            try:
                network_id = self.owned_resource_id(self.name, network=True)
            except Exception as e:
                record_error('query-network', self.name, e)
            else:
                if network_id is not None:
                    try:
                        subprocess.run(['docker', 'network', 'rm', network_id], check=True,
                                       timeout=CLEANUP_TIMEOUT_SECONDS, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    except Exception as e:
                        record_error('remove-network', self.name, e)
        self.report['cleanupErrors'] = errors
        if errors:
            self.report['passed'] = False
        return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', choices=['nifi1', 'nifi2'], required=True)
    parser.add_argument('--demo-target', type=Path, default=ROOT / 'demo/target', help='Compiled demo directory containing classes/ and lib/')
    parser.add_argument('--nar', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True, help='New directory under ignored dist/')
    parser.add_argument('--subnet', default='172.30.248.0/24', help='Unused Docker subnet; choose a different one for concurrent runs')
    parser.add_argument('--tags', type=int, default=1000)
    parser.add_argument('--seconds', type=int, default=1800,
                        help='Post-recovery soak duration in seconds; 0 skips the soak')
    parser.add_argument('--max-heap-mib', type=positive_limit,
                        help='Optional per-node ceiling on sampled usedHeapBytes during the soak (MiB = 1048576 bytes)')
    parser.add_argument('--max-threads', type=positive_limit,
                        help='Optional per-node ceiling on sampled JVM totalThreads during the soak (integer metric)')
    parser.add_argument('--etl-only', action='store_true', help='Run default delivery and ETL output/configuration checks instead of recovery/load scenarios')
    args = parser.parse_args()
    if not 1 <= args.tags <= 10000 or args.seconds < 0:
        parser.error('tags must be 1..10000 and seconds must be nonnegative')
    def interrupted(signum, frame):
        raise KeyboardInterrupt('Acceptance run interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    harness = Harness(args)
    try:
        harness.setup()
        harness.flow()
        if args.etl_only:
            harness.etl_output()
        else:
            harness.exercise()
        harness.report['passed'] = True
    except BaseException as e:
        harness.report['error'] = str(e)
        raise
    finally:
        harness.report['finishedAt'] = datetime.now(timezone.utc).isoformat()
        try:
            harness.save()
        finally:
            cleanup_errors = harness.cleanup()
            harness.save()
    if cleanup_errors:
        raise RuntimeError('Acceptance cleanup failed; see cleanupErrors in report.json')


if __name__ == '__main__':
    main()
