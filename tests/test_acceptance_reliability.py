"""Bounded content, soak and cleanup checks for disposable acceptance runs."""
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch


spec = importlib.util.spec_from_file_location('acceptance_reliability',
       Path(__file__).resolve().parents[1] / 'scripts/acceptance.py')
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)


def record(alias, node_id, value):
    return {'tag': alias, 'nodeId': node_id, 'value': value, 'quality': 'Good'}


def controlled_records():
    return [record('Tag1', 'ns=2;s=Load.Tag1', 100001),
            record('Tag0', 'ns=2;s=Load.Tag0', 100000),
            record('Tag0', 'ns=2;s=Load.Tag0', 110000),
            record('Tag0', 'ns=2;s=Load.Tag0', 120000),
            record('Tag1', 'ns=2;s=Load.Tag1', 110001),
            record('Tag1', 'ns=2;s=Load.Tag1', 120001),
            record('Tag0', 'ns=2;s=Load.Tag0', 130000)]


def harness_stub():
    harness = acceptance.Harness.__new__(acceptance.Harness)
    harness.node = 'node1'
    harness.source = 'source'
    harness.sink = 'sink'
    harness.connection = 'connection'
    harness.report = {}
    harness.progress_uuids = set()
    harness.save = Mock()
    harness.check = Mock()
    harness.state = Mock()
    return harness


class HarnessProvenanceTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name) / 'source'
        self.script = self.root / 'scripts/acceptance.py'
        self.script.parent.mkdir(parents=True)
        self.script_bytes = b'#!/usr/bin/env python3\nprint("acceptance fixture")\n'
        self.script.write_bytes(self.script_bytes)
        self.commit = 'a' * 40
        for name, value in (('ROOT', self.root), ('__file__', str(self.script))):
            patcher = patch.object(acceptance, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_git_uses_source_root_and_hashes_exact_script_bytes(self):
        self.assertNotEqual(self.root, Path.cwd())
        def git_output(arguments, **options):
            self.assertEqual('git', arguments[0])
            uses_source_root = arguments[1:3] == ['-C', str(self.root)]
            operation = arguments[3:] if uses_source_root else arguments[1:]
            if operation == ['rev-parse', '--show-toplevel']:
                return str(self.root if uses_source_root else Path.cwd())
            if operation == ['rev-parse', 'HEAD']:
                return self.commit if uses_source_root else 'b' * 40
            if operation == ['status', '--porcelain', '--untracked-files=normal']:
                return ''
            self.fail('Unexpected Git command: ' + repr(arguments))
        with patch.object(acceptance.subprocess, 'check_output', side_effect=git_output) as commands:
            result = acceptance.harness_provenance()
        self.assertEqual(self.commit, result['harnessCommit'])
        self.assertEqual(hashlib.sha256(self.script_bytes).hexdigest(), result['harnessSha256'])
        self.assertIs(False, result['sourceTreeDirty'])
        self.assertNotIn('harnessProvenanceError', result)
        self.assertEqual([
            call(['git', '-C', str(self.root), 'rev-parse', '--show-toplevel'], text=True,
                 stderr=subprocess.STDOUT, timeout=120),
            call(['git', '-C', str(self.root), 'rev-parse', 'HEAD'], text=True,
                 stderr=subprocess.STDOUT, timeout=120),
            call(['git', '-C', str(self.root), 'status', '--porcelain', '--untracked-files=normal'],
                 text=True, stderr=subprocess.STDOUT, timeout=120)], commands.call_args_list)

    def test_tracked_and_untracked_source_changes_are_dirty(self):
        for status in (' M scripts/acceptance.py\n', 'M  src/Changed.java\n',
                       '?? tests/new_test.py\n'):
            with self.subTest(status=status), patch.object(acceptance.subprocess, 'check_output',
                    side_effect=[str(self.root), self.commit, status]):
                result = acceptance.harness_provenance()
            self.assertIs(True, result['sourceTreeDirty'])
            self.assertEqual(self.commit, result['harnessCommit'])

    def test_git_lookup_failures_leave_commit_and_dirty_state_unknown(self):
        outputs = [str(self.root), self.commit, '']
        for failing_step in range(3):
            failure = subprocess.CalledProcessError(128, ['git'], output='metadata unavailable')
            with self.subTest(failing_step=failing_step), patch.object(acceptance.subprocess, 'check_output',
                    side_effect=outputs[:failing_step] + [failure]):
                result = acceptance.harness_provenance()
            self.assertIsNone(result['harnessCommit'])
            self.assertIsNone(result['sourceTreeDirty'])
            self.assertEqual(hashlib.sha256(self.script_bytes).hexdigest(), result['harnessSha256'])
            self.assertIn('metadata unavailable', result['harnessProvenanceError'])

    def test_missing_or_timed_out_git_is_explicitly_unavailable(self):
        for failure in (FileNotFoundError('git is unavailable'),
                        subprocess.TimeoutExpired(['git'], 120)):
            with self.subTest(failure=type(failure).__name__), patch.object(
                    acceptance.subprocess, 'check_output', side_effect=failure):
                result = acceptance.harness_provenance()
            self.assertIsNone(result['harnessCommit'])
            self.assertIsNone(result['sourceTreeDirty'])
            self.assertTrue(result['harnessProvenanceError'])

    def test_provenance_error_detail_is_bounded(self):
        with patch.object(acceptance.subprocess, 'check_output', side_effect=OSError('x' * 5000)):
            result = acceptance.harness_provenance()
        self.assertGreater(len(result['harnessProvenanceError']), 0)
        self.assertLessEqual(len(result['harnessProvenanceError']), 500)
        self.assertIsNone(result['sourceTreeDirty'])

    def test_nested_export_cannot_claim_ancestor_git_provenance(self):
        with patch.object(acceptance.subprocess, 'check_output', return_value=str(self.root.parent)) as commands:
            result = acceptance.harness_provenance()
        commands.assert_called_once()
        self.assertIsNone(result['harnessCommit'])
        self.assertIsNone(result['sourceTreeDirty'])
        self.assertTrue(result['harnessProvenanceError'])
        self.assertEqual(hashlib.sha256(self.script_bytes).hexdigest(), result['harnessSha256'])

    def test_harness_initialization_records_source_provenance(self):
        nar = self.root / 'fixture.nar'
        nar.write_bytes(b'NAR fixture')
        args = SimpleNamespace(output=self.root / 'dist/run', profile='nifi2', tags=2,
                               seconds=0, nar=nar)
        expected = {'harnessCommit': self.commit, 'harnessSha256': 'script-sha256',
                    'sourceTreeDirty': True}
        with patch.object(acceptance, 'harness_provenance', return_value=expected) as provenance, \
                patch.object(acceptance, 'command', side_effect=AssertionError('unexpected command')):
            harness = acceptance.Harness(args)
        provenance.assert_called_once_with()
        for name, value in expected.items():
            self.assertEqual(value, harness.report[name])
        self.assertEqual(hashlib.sha256(b'NAR fixture').hexdigest(), harness.report['narSha256'])


class ControlledContentTest(unittest.TestCase):
    def test_exact_mapping_and_per_tag_order_allow_independent_sampling(self):
        result = acceptance.validate_indexed_records(controlled_records(), [0, 1], 10, 13)
        self.assertEqual({'mode': 'indexed', 'stride': 10000, 'sampleRecords': 7, 'tags': 2,
                          'captureStartTick': 10, 'captureEndTick': 13, 'firstInteriorTick': 11,
                          'lastInteriorTick': 12, 'interiorTicks': 2}, result)

    def test_partial_boundary_ticks_are_excluded(self):
        records = controlled_records()[2:6]
        self.assertEqual(2, acceptance.validate_indexed_records(records, [0, 1], 10, 13)['interiorTicks'])

    def test_missing_entire_interior_tick_is_rejected(self):
        records = [row for row in controlled_records() if row['value'] not in (110000, 110001)]
        with self.assertRaisesRegex(AssertionError, 'Incomplete interior tick 11'):
            acceptance.validate_indexed_records(records, [0, 1], 10, 13)

    def test_missing_tag_is_rejected(self):
        records = [row for row in controlled_records() if row['value'] != 120001]
        with self.assertRaisesRegex(AssertionError, 'Incomplete interior tick 12'):
            acceptance.validate_indexed_records(records, [0, 1], 10, 13)

    def test_duplicate_and_regressing_ticks_are_rejected(self):
        for extra in (record('Tag0', 'ns=2;s=Load.Tag0', 130000),
                      record('Tag1', 'ns=2;s=Load.Tag1', 110001)):
            with self.subTest(value=extra['value']), self.assertRaisesRegex(AssertionError, 'Duplicate or regressing'):
                acceptance.validate_indexed_records(controlled_records() + [extra], [0, 1], 10, 13)

    def test_alias_node_and_value_mapping_are_checked_independently(self):
        for field, value, message in (('tag', 'Tag7', 'Unexpected controlled tag'),
                                      ('nodeId', 'ns=2;s=Load.Tag1', 'Wrong NodeId'),
                                      ('configuredNodeId', 'ns=2;s=Load.Tag1', 'Unexpected configured NodeId'),
                                      ('configuredNodeId', None, 'Unexpected configured NodeId'),
                                      ('value', 110001, 'Wrong value index')):
            records = controlled_records()
            records[2][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(AssertionError, message):
                acceptance.validate_indexed_records(records, [0, 1], 10, 13)

    def test_two_interior_ticks_are_required(self):
        for end_tick in (10, 11, 12):
            with self.subTest(end_tick=end_tick), self.assertRaisesRegex(AssertionError, 'two interior ticks'):
                acceptance.validate_indexed_records(controlled_records(), [0, 1], 10, end_tick)

    def test_empty_records_and_record_cap_are_rejected(self):
        for rows, message in (([], 'No controlled content'),
                              ([controlled_records()[0]] * 2048, 'record cap')):
            with self.subTest(message=message), self.assertRaisesRegex(AssertionError, message):
                acceptance.validate_indexed_records(rows, [0, 1], 10, 13)


class ControlledCaptureTest(unittest.TestCase):
    def capture_harness(self, rows, count=None):
        harness = harness_stub()
        listing = {'finished': True, 'flowFileSummaries': rows,
                   'queueSize': {'objectCount': len(rows) if count is None else count}}
        harness.api = Mock(return_value={'listingRequest': {'id': 'listing'}})
        harness._listing = Mock(return_value=listing)
        harness.wait = Mock(side_effect=lambda description, predicate, timeout: predicate())
        harness.read_content = Mock(side_effect=lambda base, row, timeout: [{'uuid': row['uuid']}])
        return harness

    def test_collects_every_flowfile_in_queue_position_order(self):
        harness = self.capture_harness([{'uuid': 'later', 'position': 2}, {'uuid': 'earlier', 'position': 1}])
        with patch.object(acceptance.time, 'monotonic', return_value=0):
            records, count = harness.capture_content()
        self.assertEqual([{'uuid': 'earlier'}, {'uuid': 'later'}], records)
        self.assertEqual(2, count)
        self.assertEqual(['earlier', 'later'], [args[0][1]['uuid'] for args in harness.read_content.call_args_list])
        harness.api.assert_called_with('node1', 'DELETE', '/flowfile-queues/connection/listing-requests/listing')

    def test_rejects_truncated_listing(self):
        harness = self.capture_harness([{'uuid': 'one', 'position': 1}], count=2)
        with patch.object(acceptance.time, 'monotonic', return_value=0), self.assertRaisesRegex(AssertionError, 'Truncated'):
            harness.capture_content()
        harness.read_content.assert_not_called()

    def test_rejects_reaching_flowfile_cap(self):
        harness = self.capture_harness([{'uuid': str(i), 'position': i} for i in range(80)])
        with patch.object(acceptance.time, 'monotonic', return_value=0), self.assertRaisesRegex(AssertionError, 'FlowFile cap'):
            harness.capture_content()
        harness.read_content.assert_not_called()

    def test_rejects_missing_or_duplicate_positions(self):
        for rows in ([{'uuid': 'one'}], [{'uuid': 'one', 'position': 1}, {'uuid': 'two', 'position': 1}]):
            harness = self.capture_harness(rows)
            with self.subTest(rows=rows), patch.object(acceptance.time, 'monotonic', return_value=0):
                with self.assertRaisesRegex(AssertionError, 'queue position'):
                    harness.capture_content()

    def test_record_cap_is_enforced_across_flowfiles(self):
        harness = self.capture_harness([{'uuid': 'one', 'position': 1}, {'uuid': 'two', 'position': 2}])
        harness.read_content = Mock(side_effect=[[{}] * 1024, [{}] * 1024])
        with patch.object(acceptance.time, 'monotonic', return_value=0), self.assertRaisesRegex(AssertionError, 'record cap'):
            harness.capture_content()
        self.assertEqual(2, harness.read_content.call_count)

    def test_collection_time_is_bounded_and_listing_is_removed(self):
        harness = self.capture_harness([{'uuid': 'one', 'position': 1}])
        with patch.object(acceptance.time, 'monotonic', side_effect=[0, 0, 0, 31, 31]):
            with self.assertRaisesRegex(AssertionError, 'elapsed=31.0s; limit=30s'):
                harness.capture_content()
        harness.read_content.assert_not_called()
        harness.api.assert_called_with('node1', 'DELETE', '/flowfile-queues/connection/listing-requests/listing')

    def test_listing_is_removed_when_content_read_fails(self):
        harness = self.capture_harness([{'uuid': 'one', 'position': 1}])
        harness.read_content.side_effect = RuntimeError('content unavailable')
        with patch.object(acceptance.time, 'monotonic', return_value=0), self.assertRaisesRegex(RuntimeError, 'content unavailable'):
            harness.capture_content()
        harness.api.assert_called_with('node1', 'DELETE', '/flowfile-queues/connection/listing-requests/listing')

    def controlled_harness(self):
        harness = harness_stub()
        harness.args = SimpleNamespace(tags=32)
        original = {'Tags': 'all 32 original tags', 'Default Sampling Interval (ms)': '1000',
                    'Publishing Interval (ms)': '1000', 'Default Monitored Item Queue Size': '1',
                    'Notification Buffer Capacity': '5000', 'Max Records Per FlowFile': '1000'}
        harness.api = Mock(side_effect=lambda node, method, path: {'component': {
            'config': {'properties': original}, 'validationStatus': 'VALID'}} if path.startswith('/processors/')
            else {'component': {'backPressureObjectThreshold': 10, 'backPressureDataSizeThreshold': '10 MB'}})
        harness.wait = Mock(return_value=True)
        harness.metrics = Mock(side_effect=[{'tick': 10}, {'tick': 14}])
        harness.source_properties = Mock()
        harness.connection_settings = Mock()
        harness.capture_content = Mock(side_effect=AssertionError('capture failed'))
        return harness, original

    def test_controlled_phase_restores_full_configuration_after_failure(self):
        harness, original = self.controlled_harness()
        with self.assertRaisesRegex(AssertionError, 'capture failed'):
            harness.controlled_content()
        temporary = harness.source_properties.call_args_list[0].args[0]
        self.assertEqual(16, len(temporary['Tags'].splitlines()))
        self.assertEqual('100', temporary['Default Sampling Interval (ms)'])
        self.assertEqual('100', temporary['Publishing Interval (ms)'])
        self.assertEqual('32', temporary['Default Monitored Item Queue Size'])
        harness.source_properties.assert_called_with(original)
        harness.connection_settings.assert_called_with({'backPressureObjectThreshold': 10,
                                                       'backPressureDataSizeThreshold': '10 MB'})
        self.assertEqual(call('source', 'RUNNING'), harness.state.call_args_list[-1])

    def test_restore_failure_does_not_replace_content_failure(self):
        harness, original = self.controlled_harness()
        harness.source_properties.side_effect = [None, RuntimeError('restore failed')]
        with self.assertRaisesRegex(AssertionError, 'capture failed'):
            harness.controlled_content()
        self.assertEqual('restore failed', harness.report['contentRestoreError'])


class ResourceBudgetTest(unittest.TestCase):
    def test_limits_require_positive_finite_numbers(self):
        for value in (0, -1, 'nan', 'inf', '-inf', 'bad', None):
            with self.subTest(value=value), self.assertRaises(acceptance.argparse.ArgumentTypeError):
                acceptance.positive_limit(value)
        self.assertEqual(12.5, acceptance.positive_limit('12.5'))

    def test_unsampled_budgets_are_not_run(self):
        for heap, threads in ((None, None), (2, 12)):
            budget = acceptance.resource_budget_report(['node1'], heap, threads)
            self.assertEqual('not_run', budget['heap']['status'])
            self.assertEqual('not_run', budget['threads']['status'])
            self.assertEqual({'maximum': None, 'samples': 0}, budget['heap']['nodes']['node1'])

    def test_budget_requires_an_expected_node(self):
        with self.assertRaisesRegex(ValueError, 'expected NiFi node'):
            acceptance.resource_budget_report([], 2, 12)

    def test_large_finite_heap_limit_does_not_overflow_to_infinity(self):
        budget = acceptance.resource_budget_report(['node1'], 1e308)
        self.assertEqual(1e308, budget['limits']['maxHeapMiB'])
        self.assertIsInstance(budget['heap']['limit'], int)
        self.assertTrue(acceptance.observe_resource_budgets(budget, {'node1': {'usedHeapBytes': 1048576}}))
        self.assertEqual('passed', budget['heap']['status'])

    def test_mib_conversion_and_per_node_maximum_aggregation(self):
        budget = acceptance.resource_budget_report(['node1', 'node2'], 2, 12)
        self.assertEqual(2097152, budget['heap']['limit'])
        self.assertTrue(acceptance.observe_resource_budgets(budget, {
            'node1': {'usedHeapBytes': 1048576, 'totalThreads': 8},
            'node2': {'usedHeapBytes': 2097152, 'totalThreads': 12}}))
        self.assertTrue(acceptance.observe_resource_budgets(budget, {
            'node1': {'usedHeapBytes': 524288, 'totalThreads': 10},
            'node2': {'usedHeapBytes': 1572864, 'totalThreads': 9}}))
        self.assertEqual({'maximum': 1048576, 'samples': 2}, budget['heap']['nodes']['node1'])
        self.assertEqual({'maximum': 2097152, 'samples': 2}, budget['heap']['nodes']['node2'])
        self.assertEqual({'maximum': 10, 'samples': 2}, budget['threads']['nodes']['node1'])
        self.assertEqual({'maximum': 12, 'samples': 2}, budget['threads']['nodes']['node2'])
        self.assertEqual('passed', budget['heap']['status'])
        self.assertEqual('passed', budget['threads']['status'])

    def test_observations_without_limits_are_not_budget_assertions(self):
        budget = acceptance.resource_budget_report(['node1'])
        self.assertTrue(acceptance.observe_resource_budgets(budget, {'node1': {'usedHeapBytes': 1048576, 'totalThreads': 8}}))
        self.assertEqual('not_configured', budget['heap']['status'])
        self.assertEqual('not_configured', budget['threads']['status'])
        self.assertEqual({'maximum': 8, 'samples': 1}, budget['threads']['nodes']['node1'])

    def test_exceeded_bounds_fail_independently_and_remain_failed(self):
        for resources, failed, passed in (({'usedHeapBytes': 2097153, 'totalThreads': 12}, 'heap', 'threads'),
                                         ({'usedHeapBytes': 2097152, 'totalThreads': 13}, 'threads', 'heap')):
            budget = acceptance.resource_budget_report(['node1'], 2, 12)
            with self.subTest(failed=failed):
                self.assertFalse(acceptance.observe_resource_budgets(budget, {'node1': resources}))
                self.assertEqual('failed', budget[failed]['status'])
                self.assertEqual('passed', budget[passed]['status'])
                self.assertIn('exceeds', budget[failed]['lastError'])
                self.assertFalse(acceptance.observe_resource_budgets(budget, {
                    'node1': {'usedHeapBytes': 1024, 'totalThreads': 1}}))
                self.assertEqual('failed', budget[failed]['status'])

    def test_missing_expected_node_fails_instead_of_using_aggregate(self):
        budget = acceptance.resource_budget_report(['node1', 'node2'], 2, 12)
        self.assertFalse(acceptance.observe_resource_budgets(budget, {
            'node1': {'usedHeapBytes': 100, 'totalThreads': 8},
            'aggregate': {'usedHeapBytes': 200, 'totalThreads': 9}}))
        for name in ('heap', 'threads'):
            self.assertEqual('failed', budget[name]['status'])
            self.assertIn('node2', budget[name]['lastError'])
            self.assertEqual({'maximum': None, 'samples': 0}, budget[name]['nodes']['node2'])

    def test_missing_and_invalid_required_metrics_fail(self):
        for metric, name in (('usedHeapBytes', 'heap'), ('totalThreads', 'threads')):
            for invalid in (None, float('nan'), float('inf'), -1, True, '8', 8.5):
                budget = acceptance.resource_budget_report(['node1'], 2, 12)
                sample = {'usedHeapBytes': 100, 'totalThreads': 8, metric: invalid}
                with self.subTest(metric=metric, invalid=invalid):
                    self.assertFalse(acceptance.observe_resource_budgets(budget, {'node1': sample}))
                    self.assertEqual('failed', budget[name]['status'])
                    self.assertEqual({'maximum': None, 'samples': 0}, budget[name]['nodes']['node1'])
            budget = acceptance.resource_budget_report(['node1'], 2, 12)
            sample = {'usedHeapBytes': 100, 'totalThreads': 8}
            del sample[metric]
            self.assertFalse(acceptance.observe_resource_budgets(budget, {'node1': sample}))

    def test_total_threads_must_be_a_positive_integer(self):
        for value in (0, 8.5):
            budget = acceptance.resource_budget_report(['node1'], max_threads=12.5)
            with self.subTest(value=value):
                self.assertFalse(acceptance.observe_resource_budgets(budget, {'node1': {'totalThreads': value}}))
        budget = acceptance.resource_budget_report(['node1'], max_threads=12.5)
        self.assertTrue(acceptance.observe_resource_budgets(budget, {'node1': {'totalThreads': 12}}))
        self.assertEqual('passed', budget['threads']['status'])
        self.assertEqual('not_configured', budget['heap']['status'])


class FakeClock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class FinalStopTest(unittest.TestCase):
    def final_harness(self, state='STOPPED', active_threads=0):
        harness = harness_stub()
        harness.api = Mock(return_value={'component': {'state': state},
            'status': {'aggregateSnapshot': {'activeThreadCount': active_threads}}})
        harness.metrics = Mock(return_value={'sessions': 0, 'subscriptions': 0,
                                             'monitoredItems': 0, 'tick': 10})
        harness.snapshot = Mock()
        return harness

    def test_processor_must_be_stopped_with_zero_active_threads(self):
        for state, active_threads in (('RUNNING', 0), ('STOPPING', 0), ('STOPPED', 1)):
            harness = self.final_harness(state, active_threads)
            with self.subTest(state=state, active_threads=active_threads):
                self.assertFalse(harness.stopped_resources())
            harness.api.assert_called_once_with('node1', 'GET', '/processors/source')
            harness.metrics.assert_called_once_with()

    def test_active_thread_count_requires_an_integer_zero(self):
        missing = object()
        for value in (missing, None, -1, 0.0, 0.5, '0', False, True):
            harness = self.final_harness(active_threads=value)
            if value is missing:
                del harness.api.return_value['status']['aggregateSnapshot']['activeThreadCount']
            with self.subTest(value=value):
                self.assertFalse(harness.stopped_resources())

    def test_each_server_resource_requires_an_integer_zero(self):
        missing = object()
        for resource in ('sessions', 'subscriptions', 'monitoredItems'):
            for value in (missing, None, -1, 1, 0.0, 0.5, '0', False, True):
                harness = self.final_harness()
                if value is missing:
                    del harness.metrics.return_value[resource]
                else:
                    harness.metrics.return_value[resource] = value
                with self.subTest(resource=resource, value=value):
                    self.assertFalse(harness.stopped_resources())
                harness.metrics.assert_called_once_with()

    def test_all_resource_counts_come_from_one_metrics_snapshot(self):
        harness = self.final_harness()
        harness.metrics.side_effect = [
            {'sessions': 0, 'subscriptions': 1, 'monitoredItems': 1},
            {'sessions': 1, 'subscriptions': 0, 'monitoredItems': 1},
            {'sessions': 1, 'subscriptions': 1, 'monitoredItems': 0}]
        self.assertFalse(harness.stopped_resources())
        harness.metrics.assert_called_once_with()

    def test_success_returns_the_exact_combined_observation(self):
        harness = self.final_harness()
        observed = harness.stopped_resources()
        self.assertEqual({'processorStopped': True, 'server': harness.metrics.return_value}, observed)
        self.assertIs(harness.metrics.return_value, observed['server'])
        harness.api.assert_called_once_with('node1', 'GET', '/processors/source')
        harness.metrics.assert_called_once_with()

    def test_final_stop_records_the_successful_wait_observation(self):
        harness = self.final_harness()
        successful = {'sessions': 0, 'subscriptions': 0, 'monitoredItems': 0, 'tick': 12}
        harness.metrics.side_effect = [
            {'sessions': 1, 'subscriptions': 0, 'monitoredItems': 0, 'tick': 11},
            successful,
            {'sessions': 1, 'subscriptions': 1, 'monitoredItems': 1, 'tick': 13}]
        clock = FakeClock()
        with patch.object(acceptance.time, 'monotonic', clock.monotonic), \
                patch.object(acceptance.time, 'sleep', clock.sleep), redirect_stdout(io.StringIO()):
            harness.final_stop()
        harness.state.assert_called_once_with('source', 'STOPPED')
        harness.snapshot.assert_called_once_with('stopped')
        harness.check.assert_called_once_with('final-stop-cleanup',
            {'processorStopped': True, 'server': successful})
        self.assertIs(successful, harness.check.call_args.args[1]['server'])
        self.assertEqual(2, harness.metrics.call_count)
        self.assertEqual(2, clock.now)

    def test_each_blocked_condition_times_out_without_a_success_marker(self):
        for blocked in ('state', 'activeThreads', 'sessions', 'subscriptions', 'monitoredItems'):
            harness = self.final_harness(state='RUNNING' if blocked == 'state' else 'STOPPED',
                                         active_threads=1 if blocked == 'activeThreads' else 0)
            if blocked in harness.metrics.return_value:
                harness.metrics.return_value[blocked] = 1
            clock = FakeClock()
            with self.subTest(blocked=blocked):
                with patch.object(acceptance.time, 'monotonic', clock.monotonic), \
                        patch.object(acceptance.time, 'sleep', clock.sleep), redirect_stdout(io.StringIO()):
                    with self.assertRaisesRegex(RuntimeError, 'final stop cleanup; elapsed=30.0s; limit=30s'):
                        harness.final_stop()
                self.assertEqual(30, clock.now)
                self.assertEqual(30, harness.report['progress']['limitSeconds'])
                harness.state.assert_called_once_with('source', 'STOPPED')
                harness.snapshot.assert_not_called()
                harness.check.assert_not_called()

    def test_stop_and_resources_must_be_ready_in_the_same_attempt(self):
        harness = self.final_harness()
        running = {'component': {'state': 'RUNNING'},
                   'status': {'aggregateSnapshot': {'activeThreadCount': 0}}}
        stopped = {'component': {'state': 'STOPPED'},
                   'status': {'aggregateSnapshot': {'activeThreadCount': 0}}}
        harness.api.side_effect = [running, stopped] * 8
        harness.metrics.side_effect = [
            {'sessions': 0, 'subscriptions': 0, 'monitoredItems': 0},
            {'sessions': 1, 'subscriptions': 0, 'monitoredItems': 0}] * 8
        clock = FakeClock()
        with patch.object(acceptance.time, 'monotonic', clock.monotonic), \
                patch.object(acceptance.time, 'sleep', clock.sleep), redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, 'final stop cleanup'):
                harness.final_stop()
        self.assertEqual(30, clock.now)
        self.assertEqual(harness.api.call_count, harness.metrics.call_count)
        harness.check.assert_not_called()

    def test_exercise_runs_the_combined_final_stop_gate(self):
        harness = self.final_harness()
        harness.nodes = ['node1']
        harness.server = 'server'
        harness.controlled_content = Mock()
        harness.graceful_stop = Mock()
        harness.delivery_progress = Mock()
        harness.soak = Mock()
        harness.metrics.return_value['sessions'] = 1
        def wait(description, predicate, timeout=180):
            if description == 'final stop cleanup':
                return acceptance.Harness.wait(harness, description, predicate, timeout)
            return True
        harness.wait = Mock(side_effect=wait)
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as directory:
            harness.security = Path(directory)
            with patch.object(acceptance, 'command', return_value=''), \
                    patch.object(acceptance.time, 'monotonic', clock.monotonic), \
                    patch.object(acceptance.time, 'sleep', clock.sleep), redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, 'final stop cleanup; elapsed=30.0s; limit=30s'):
                    harness.exercise()
        harness.soak.assert_called_once_with()
        self.assertGreater(harness.api.call_count, 0)
        self.assertNotIn(call('stopped'), harness.snapshot.call_args_list)
        self.assertNotIn('final-stop-cleanup', [entry.args[0] for entry in harness.check.call_args_list])
        harness.state.assert_called_with('source', 'STOPPED')


class PhaseWaitTest(unittest.TestCase):
    def test_positive_result_after_deadline_cannot_pass_final_stop(self):
        harness = FinalStopTest().final_harness()
        clock = FakeClock()
        def slow_observation():
            clock.sleep(31)
            return {'processorStopped': True}
        harness.stopped_resources = slow_observation
        with patch.object(acceptance.time, 'monotonic', clock.monotonic), \
                patch.object(acceptance.time, 'sleep', clock.sleep), redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, 'after the deadline'):
                harness.final_stop()
        harness.check.assert_not_called()
        harness.snapshot.assert_not_called()

    def test_nested_wait_and_http_requests_share_the_outer_budget(self):
        harness = harness_stub()
        clock = FakeClock()
        harness.ports = {'node1': '8080'}
        def observation():
            clock.sleep(3)
            harness.api('node1', 'GET', '/test')
            return False
        with patch.object(acceptance.time, 'monotonic', clock.monotonic), \
                patch.object(acceptance.time, 'sleep', clock.sleep), \
                patch.object(acceptance.urllib.request, 'urlopen') as open_url, redirect_stdout(io.StringIO()):
            open_url.return_value.__enter__.return_value.read.return_value = b'{}'
            with self.assertRaisesRegex(RuntimeError, 'outer; elapsed=5.0s'):
                harness.wait('outer', lambda: harness.wait('inner', observation, 180), 5)
            self.assertEqual(1, open_url.call_count)
            self.assertEqual(2, open_url.call_args.kwargs['timeout'])
            self.assertEqual(20, harness.request_timeout(20))

    def test_timeout_reports_phase_elapsed_limit_and_last_progress(self):
        harness = harness_stub()
        clock = FakeClock()
        with patch.object(acceptance.time, 'monotonic', clock.monotonic), patch.object(acceptance.time, 'sleep', clock.sleep), redirect_stdout(io.StringIO()) as output:
            with self.assertRaisesRegex(RuntimeError, 'source ready; elapsed=5.0s; limit=5s; last progress=False'):
                harness.wait('source ready', lambda: False, 5)
        self.assertEqual({'phase': 'source ready', 'elapsedSeconds': 5, 'limitSeconds': 5,
                          'lastProgress': 'False'}, harness.report['progress'])
        self.assertIn('Waiting:', output.getvalue())
        self.assertEqual(5, clock.now)


class ClusterOwnershipTest(unittest.TestCase):
    def harness(self):
        h = harness_stub()
        h.nodes = ['node1', 'node2']
        h.connected = Mock(return_value=[
            {'address': 'node1', 'nodeId': 'first', 'roles': ['Primary Node']},
            {'address': 'node2', 'nodeId': 'second', 'roles': []}])
        h.api = Mock(return_value={'component': {'loadBalanceStrategy': 'DO_NOT_LOAD_BALANCE',
                                                 'backPressureObjectThreshold': 10,
                                                 'backPressureDataSizeThreshold': '10 MB'},
                                   'listingRequest': {'id': 'listing'}})
        h.connection_settings = Mock()
        h.queue_count_now = Mock(return_value=0)
        h.stopped = Mock(return_value=True)
        h.metrics = Mock(side_effect=[{'subscriptions': 1, 'tick': 10}, {'subscriptions': 1, 'tick': 15}])
        return h

    def listing(self, *rows):
        return {'flowFileSummaries': [{'uuid': uuid, 'clusterNodeId': owner} for uuid, owner in rows],
                'queueSize': {'objectCount': len(rows)}}

    def test_only_new_uuids_count_for_their_origin_node(self):
        h = self.harness()
        h._listing = Mock(side_effect=[self.listing(('old', 'second')),
            self.listing(('old', 'second'), ('fresh', 'first'))])
        with patch.object(acceptance.time, 'sleep'):
            h.primary_ownership()
        h.check.assert_called_once_with('primary-only-delivery',
            {'primary': 'node1', 'freshFlowFiles': {'node1': 1, 'node2': 0}})
        self.assertEqual([call('sink', 'STOPPED'), call('sink', 'RUNNING')], h.state.call_args_list)

    def test_primary_can_add_a_new_uuid_after_filling_the_baseline_queue(self):
        h = self.harness()
        rows = [(str(i), 'first') for i in range(10)]
        def snapshot(base, lid):
            has_room = h.connection_settings.call_count and h.connection_settings.call_args.args[0]['backPressureObjectThreshold'] > 10
            return self.listing(*(rows + ([('fresh', 'first')] if has_room else [])))
        h._listing = Mock(side_effect=snapshot)
        with patch.object(acceptance.time, 'sleep'):
            h.primary_ownership()
        self.assertEqual(1, h.check.call_args.args[1]['freshFlowFiles']['node1'])
        h.connection_settings.assert_called_with({'backPressureObjectThreshold': 10, 'backPressureDataSizeThreshold': '10 MB'})

    def test_flowfile_appended_while_nifi_collects_listing_rows(self):
        h = self.harness()
        listing = self.listing(('old', 'first'), ('appended', 'first'))
        listing['queueSize']['objectCount'] = 1
        h._listing = Mock(return_value=listing)
        self.assertEqual({'old': 'first', 'appended': 'first'}, h.queued_owners({'first', 'second'}))

    def test_ambiguous_or_standby_delivery_cannot_pass(self):
        for case in ('truncated', 'duplicate', 'unknown', 'moved', 'missing', 'unchanged', 'standby',
                     'role change', 'missing node', 'load balanced', 'listing failed', 'stale server metrics',
                     'listing cap'):
            with self.subTest(case=case):
                h = self.harness()
                last = self.listing(('old', 'first'), ('fresh', 'first'))
                if case == 'truncated':
                    last['queueSize']['objectCount'] += 1
                elif case == 'duplicate':
                    last = self.listing(('old', 'first'), ('old', 'first'))
                elif case == 'listing cap':
                    last = self.listing(('old', 'first'), *((str(i), 'first') for i in range(99)))
                elif case in ('unknown', 'standby'):
                    last = self.listing(('old', 'first'), ('fresh', 'third' if case == 'unknown' else 'second'))
                elif case == 'moved':
                    last = self.listing(('old', 'second'), ('fresh', 'first'))
                elif case == 'missing':
                    last = self.listing(('fresh', 'first'))
                elif case == 'unchanged':
                    last = self.listing(('old', 'first'))
                elif case == 'listing failed':
                    last['failureReason'] = 'listing unavailable'
                elif case == 'stale server metrics':
                    h.metrics.side_effect = [{'subscriptions': 1, 'tick': 10}] * 2
                elif case in ('role change', 'missing node'):
                    original = h.connected.return_value
                    changed = [dict(original[0], roles=[]), dict(original[1], roles=['Primary Node'])]
                    h.connected.side_effect = [original, changed if case == 'role change' else original[:1]]
                else:
                    h.api.return_value['component']['loadBalanceStrategy'] = 'ROUND_ROBIN'
                h._listing = Mock(side_effect=[self.listing(('old', 'first')), last])
                with patch.object(acceptance.time, 'sleep'), self.assertRaises(AssertionError):
                    h.primary_ownership()
                h.check.assert_not_called()
                if case != 'load balanced':
                    h.state.assert_called_with('sink', 'RUNNING')


class SoakProgressTest(unittest.TestCase):
    def soak_harness(self, seconds):
        harness = harness_stub()
        harness.args = SimpleNamespace(seconds=seconds)
        harness.report['resourceBudgets'] = acceptance.resource_budget_report(['node1'], 2, 12)
        harness.snapshot = Mock(return_value={'server': {'subscriptions': 1},
            'resources': {'node1': {'usedHeapBytes': 1024, 'totalThreads': 8}}})
        harness.delivery_progress = Mock(return_value={'flowFileUuid': 'fresh', 'newestSourceTimestamp': 1700000000})
        return harness

    def test_checks_fresh_delivery_periodically_and_aggregates_samples(self):
        harness = self.soak_harness(21)
        clock = FakeClock()
        with patch.object(acceptance.time, 'monotonic', clock.monotonic), patch.object(acceptance.time, 'sleep', clock.sleep), redirect_stdout(io.StringIO()):
            harness.soak()
        self.assertEqual(3, harness.snapshot.call_count)
        self.assertEqual([call('soak fresh delivery', record_check=False)] * 3, harness.delivery_progress.call_args_list)
        self.assertEqual(3, harness.report['soakProgress']['checks'])
        self.assertEqual({'maximum': 1024, 'samples': 3}, harness.report['resourceBudgets']['heap']['nodes']['node1'])
        self.assertEqual(21, clock.now)
        self.assertEqual(1, harness.check.call_count)

    def test_zero_seconds_has_no_soak_samples_or_freshness_probe(self):
        harness = self.soak_harness(0)
        clock = FakeClock()
        with patch.object(acceptance.time, 'monotonic', clock.monotonic):
            harness.soak()
        harness.snapshot.assert_not_called()
        harness.delivery_progress.assert_not_called()
        self.assertEqual('not_run', harness.report['resourceBudgets']['heap']['status'])
        self.assertEqual('not_run', harness.report['resourceBudgets']['threads']['status'])
        self.assertEqual(0, harness.report['soakProgress']['checks'])

    def test_soak_fails_on_missing_budget_metric(self):
        harness = self.soak_harness(21)
        harness.snapshot.return_value['resources']['node1'].pop('totalThreads')
        clock = FakeClock()
        with patch.object(acceptance.time, 'monotonic', clock.monotonic):
            with self.assertRaisesRegex(AssertionError, 'Soak resource budget failed.*totalThreads'):
                harness.soak()
        self.assertEqual('failed', harness.report['resourceBudgets']['threads']['status'])
        harness.delivery_progress.assert_not_called()

    def test_soak_fails_on_fresh_delivery_failure(self):
        harness = self.soak_harness(21)
        harness.delivery_progress.side_effect = AssertionError('stale source')
        clock = FakeClock()
        with patch.object(acceptance.time, 'monotonic', clock.monotonic):
            with self.assertRaisesRegex(AssertionError, 'stale source'):
                harness.soak()
        self.assertEqual(0, harness.report['soakProgress']['checks'])

    def test_snapshot_history_is_bounded(self):
        harness = harness_stub()
        harness.report['samples'] = []
        harness.api = Mock(side_effect=lambda node, method, path: {'processorStatus': {}} if 'processors/' in path
            else {'systemDiagnostics': {'aggregateSnapshot': {}}} if 'diagnostics' in path else {})
        harness.metrics = Mock(return_value={})
        with patch.object(acceptance.time, 'time', side_effect=range(258)):
            for _ in range(258):
                harness.snapshot('soak')
        self.assertEqual(256, len(harness.report['samples']))
        self.assertEqual(2, harness.report['samples'][0]['at'])
        self.assertEqual(257, harness.report['samples'][-1]['at'])


class FreshDeliveryControlTest(unittest.TestCase):
    def progress_harness(self):
        harness = harness_stub()
        harness.wait = Mock(side_effect=lambda description, predicate, timeout: predicate())
        harness.queue_count_now = Mock(side_effect=[0, 1])
        harness.queued = Mock(side_effect=AssertionError('cached counts must not prove freshness'))
        harness.stopped = Mock(return_value=True)
        harness.inspect_content = Mock(return_value={'flowFileUuid': 'fresh', 'newestSourceTimestamp': 1700000000})
        return harness

    def test_uses_fresh_queue_state_and_keeps_only_latest_uuid(self):
        harness = self.progress_harness()
        harness.progress_uuids = {'previous'}
        result = harness.delivery_progress('progress', record_check=False)
        self.assertEqual('fresh', result['flowFileUuid'])
        self.assertEqual({'fresh'}, harness.progress_uuids)
        self.assertEqual([call('sink', 'STOPPED'), call('sink', 'RUNNING')], harness.state.call_args_list)
        self.assertEqual(2, harness.queue_count_now.call_count)
        harness.queued.assert_not_called()
        harness.check.assert_not_called()

    def test_repeated_uuid_does_not_count_as_delivery_progress(self):
        harness = self.progress_harness()
        harness.progress_uuids = {'fresh'}
        with self.assertRaisesRegex(AssertionError, 'Repeated FlowFile'):
            harness.delivery_progress('progress')
        harness.state.assert_called_with('sink', 'RUNNING')

    def test_content_failure_still_restarts_sink(self):
        harness = self.progress_harness()
        harness.inspect_content.side_effect = AssertionError('stale content')
        with self.assertRaisesRegex(AssertionError, 'stale content'):
            harness.delivery_progress('progress')
        harness.state.assert_called_with('sink', 'RUNNING')


class CleanupControlTest(unittest.TestCase):
    def cleanup_harness(self):
        harness = harness_stub()
        harness.name = 'run'
        harness.containers = []
        harness.network_attempted = False
        harness.report['passed'] = True
        return harness

    def test_collection_failures_do_not_prevent_removal_of_all_resources(self):
        harness = self.cleanup_harness()
        harness.containers = ['run-opcua', 'run-node1', 'run-node2']
        harness.network_attempted = True
        def command(*args):
            if args[:2] == ('docker', 'ps'):
                return 'opcua-id run-opcua\nnode1-id run-node1\nnode2-id run-node2'
            if args[:3] == ('docker', 'network', 'ls'):
                return 'network-id run'
            if args == ('docker', 'logs', 'node2-id'):
                raise RuntimeError('logs unavailable')
            if args == ('docker', 'rm', '-f', '-v', 'node2-id'):
                raise RuntimeError('remove failed')
            return 'collected logs'
        def run(args, **options):
            self.assertEqual(30, options['timeout'])
            self.assertTrue(options['check'])
            if args[:2] == ['docker', 'cp'] and args[2].startswith('node2-id:'):
                raise subprocess.TimeoutExpired(args, 30)
            if args[:3] == ['docker', 'network', 'rm']:
                raise subprocess.CalledProcessError(1, args)
        with tempfile.TemporaryDirectory() as directory:
            harness.out = Path(directory)
            with patch.object(acceptance, 'command', side_effect=command) as commands, patch.object(acceptance.subprocess, 'run', side_effect=run) as runs, redirect_stdout(io.StringIO()):
                errors = harness.cleanup()
        self.assertEqual(['logs', 'copy-log', 'remove-container', 'remove-network'], [error['action'] for error in errors])
        for resource_id in ('node2-id', 'node1-id', 'opcua-id'):
            self.assertIn(call('docker', 'rm', '-f', '-v', resource_id), commands.call_args_list)
        self.assertEqual(3, runs.call_count)
        self.assertEqual(['docker', 'network', 'rm', 'network-id'], runs.call_args_list[-1].args[0])
        self.assertFalse(harness.report['passed'])
        self.assertEqual(errors, harness.report['cleanupErrors'])

    def test_container_timeout_after_creation_still_removes_verified_id(self):
        harness = self.cleanup_harness()
        def command(*args):
            if args[:2] == ('docker', 'run'):
                self.assertEqual(['run-opcua'], harness.containers)
                self.assertIn('opcua.acceptance=run', args)
                raise RuntimeError('docker run timed out after creation')
            if args[:2] == ('docker', 'ps'):
                self.assertIn('label=opcua.acceptance=run', args)
                return 'created-id run-opcua'
            return 'ok'
        with tempfile.TemporaryDirectory() as directory:
            harness.out = Path(directory)
            with patch.object(acceptance, 'command', side_effect=command) as commands, patch.object(acceptance.subprocess, 'run') as runs:
                with self.assertRaisesRegex(RuntimeError, 'timed out after creation'):
                    harness.run('opcua', 'demo-image')
                self.assertEqual([], harness.cleanup())
        self.assertIn(call('docker', 'logs', 'created-id'), commands.call_args_list)
        self.assertIn(call('docker', 'rm', '-f', '-v', 'created-id'), commands.call_args_list)
        runs.assert_not_called()

    def test_network_timeout_after_creation_still_removes_verified_id(self):
        harness = self.cleanup_harness()
        harness.args = SimpleNamespace(subnet='172.30.248.0/24')
        def command(*args):
            if args[:3] == ('docker', 'network', 'create'):
                self.assertTrue(harness.network_attempted)
                self.assertIn('--label', args)
                self.assertIn('opcua.acceptance=run', args)
                raise RuntimeError('network create timed out after creation')
            self.assertEqual(('docker', 'network', 'ls'), args[:3])
            self.assertIn('label=opcua.acceptance=run', args)
            return 'created-network-id run'
        with patch.object(acceptance, 'command', side_effect=command), patch.object(acceptance.subprocess, 'run') as runs:
            with self.assertRaisesRegex(RuntimeError, 'timed out after creation'):
                harness.setup()
            self.assertEqual([], harness.cleanup())
        runs.assert_called_once_with(['docker', 'network', 'rm', 'created-network-id'], check=True,
            timeout=30, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def test_precreation_failure_and_confirmed_absence_need_no_cleanup_actions(self):
        for network in (False, True):
            harness = self.cleanup_harness()
            harness.args = SimpleNamespace(subnet='172.30.248.0/24')
            def command(*args):
                if args[:2] == ('docker', 'run') or args[:3] == ('docker', 'network', 'create'):
                    raise RuntimeError('failed before creation')
                self.assertTrue(args[:2] == ('docker', 'ps') or args[:3] == ('docker', 'network', 'ls'))
                self.assertIn('label=opcua.acceptance=run', args)
                return ''
            with self.subTest(network=network), patch.object(acceptance, 'command', side_effect=command) as commands, patch.object(acceptance.subprocess, 'run') as runs:
                with self.assertRaisesRegex(RuntimeError, 'failed before creation'):
                    harness.setup() if network else harness.run('opcua', 'demo-image')
                self.assertEqual([], harness.cleanup())
            self.assertEqual(2, commands.call_count)
            runs.assert_not_called()
            self.assertTrue(harness.report['passed'])

    def test_foreign_label_and_unattempted_names_are_untouched(self):
        harness = self.cleanup_harness()
        harness.containers = ['run-node1']
        harness.network_attempted = True
        containers = [('foreign-container', 'run-node1', 'another-run'),
                      ('unattempted-container', 'run-node10', 'run')]
        networks = [('foreign-network', 'run', 'another-run'), ('unattempted-network', 'run-other', 'run')]
        def command(*args):
            self.assertIn('label=opcua.acceptance=run', args)
            if args[:2] == ('docker', 'ps'):
                resources = containers
                self.assertIn('{{.ID}} {{.Names}}', args)
            else:
                self.assertEqual(('docker', 'network', 'ls'), args[:3])
                resources = networks
                self.assertIn('{{.ID}} {{.Name}}', args)
            return '\n'.join(resource_id + ' ' + name for resource_id, name, label in resources if label == 'run')
        with patch.object(acceptance, 'command', side_effect=command) as commands, patch.object(acceptance.subprocess, 'run') as runs:
            self.assertEqual([], harness.cleanup())
        self.assertEqual(2, commands.call_count)
        runs.assert_not_called()

    def test_query_and_removal_errors_do_not_prevent_later_cleanup(self):
        harness = self.cleanup_harness()
        harness.containers = ['run-opcua', 'run-node1']
        harness.network_attempted = True
        queries = 0
        def command(*args):
            nonlocal queries
            if args[:2] == ('docker', 'ps'):
                queries += 1
                if queries == 1:
                    raise RuntimeError('container query unavailable')
                return 'opcua-id run-opcua'
            if args[:3] == ('docker', 'network', 'ls'):
                return 'network-id run'
            if args[:2] == ('docker', 'rm'):
                raise RuntimeError('container removal unavailable')
            return 'collected logs'
        with tempfile.TemporaryDirectory() as directory:
            harness.out = Path(directory)
            with patch.object(acceptance, 'command', side_effect=command) as commands, patch.object(acceptance.subprocess, 'run') as runs, redirect_stdout(io.StringIO()):
                errors = harness.cleanup()
        self.assertEqual(['query-container', 'remove-container'], [error['action'] for error in errors])
        self.assertEqual(['run-node1', 'run-opcua'], [error['resource'] for error in errors])
        self.assertEqual(2, queries)
        self.assertIn(call('docker', 'rm', '-f', '-v', 'opcua-id'), commands.call_args_list)
        self.assertEqual(['docker', 'network', 'rm', 'network-id'], runs.call_args.args[0])
        self.assertFalse(harness.report['passed'])

    def test_network_query_failure_is_recorded_after_container_removal(self):
        harness = self.cleanup_harness()
        harness.containers = ['run-opcua']
        harness.network_attempted = True
        def command(*args):
            if args[:2] == ('docker', 'ps'):
                return 'opcua-id run-opcua'
            if args[:3] == ('docker', 'network', 'ls'):
                raise RuntimeError('network query unavailable')
            return 'ok'
        with tempfile.TemporaryDirectory() as directory:
            harness.out = Path(directory)
            with patch.object(acceptance, 'command', side_effect=command) as commands, patch.object(acceptance.subprocess, 'run') as runs, redirect_stdout(io.StringIO()):
                errors = harness.cleanup()
        self.assertEqual(['query-network'], [error['action'] for error in errors])
        self.assertIn(call('docker', 'rm', '-f', '-v', 'opcua-id'), commands.call_args_list)
        runs.assert_not_called()
        self.assertFalse(harness.report['passed'])

    def test_subprocess_commands_have_a_fixed_timeout(self):
        with patch.object(acceptance.subprocess, 'check_output', return_value='done\n') as run:
            self.assertEqual('done', acceptance.command('docker', 'logs', 'run-node1'))
        self.assertEqual(120, run.call_args.kwargs['timeout'])

    def test_subprocess_timeout_has_a_useful_error(self):
        with patch.object(acceptance.subprocess, 'check_output', side_effect=subprocess.TimeoutExpired(['docker', 'logs'], 120)):
            with self.assertRaisesRegex(RuntimeError, 'docker logs timed out; limit=120s'):
                acceptance.command('docker', 'logs', 'run-node1')


class DisposableContainerTest(unittest.TestCase):
    def test_each_disposable_container_has_memory_and_cpu_caps(self):
        for profile, expected in (('nifi1', {'opcua': '1g', 'zk': '512m', 'node1': '3g', 'node2': '3g'}),
                                  ('nifi2', {'opcua': '1g', 'node1': '3g'})):
            harness = harness_stub()
            harness.name = 'run'
            harness.ports = {}
            harness.args = SimpleNamespace(profile=profile, subnet='172.30.248.0/24', tags=2,
                demo_target=Path('/tmp/demo'), nar=Path('/tmp/example.nar'))
            harness.run = Mock(side_effect=lambda suffix, *args: 'run-' + suffix)
            harness.wait = Mock(return_value=True)
            def command(*args):
                if args[:2] == ('docker', 'port'):
                    return '127.0.0.1:49152'
                if args[:2] == ('docker', 'inspect'):
                    return '[{"Image":"image-id"}]'
                return 'ok'
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as directory:
                harness.security = Path(directory)
                with patch.object(acceptance, 'command', side_effect=command):
                    harness.setup()
            self.assertEqual(set(expected), {invocation.args[0] for invocation in harness.run.call_args_list})
            for invocation in harness.run.call_args_list:
                suffix, image, options = invocation.args[:3]
                self.assertEqual(expected[suffix], options[options.index('--memory') + 1])
                self.assertEqual('2', options[options.index('--cpus') + 1])
                if suffix == 'opcua':
                    self.assertIn('DEMO_LOAD_VALUE_MODE=indexed', options)


class AcceptanceMainTest(unittest.TestCase):
    def run_main(self, harness, *extra):
        argv = ['acceptance.py', '--profile', 'nifi2', '--nar', '/tmp/example.nar', '--output', '/tmp/example', *extra]
        with patch('sys.argv', argv), patch.object(acceptance, 'Harness', return_value=harness), patch.object(acceptance.signal, 'signal'):
            acceptance.main()

    def test_primary_failure_survives_cleanup_errors(self):
        harness = Mock()
        harness.report = {}
        harness.setup.side_effect = RuntimeError('primary failure')
        harness.cleanup.return_value = [{'error': 'cleanup failure'}]
        with self.assertRaisesRegex(RuntimeError, '^primary failure$'):
            self.run_main(harness)
        self.assertEqual('primary failure', harness.report['error'])
        harness.cleanup.assert_called_once()

    def test_setup_creation_timeout_survives_ownership_query_failure(self):
        harness = harness_stub()
        harness.name = 'run'
        harness.containers = []
        harness.network_attempted = False
        harness.setup = lambda: harness.run('opcua', 'demo-image')
        def command(*args):
            if args[:2] == ('docker', 'run'):
                raise RuntimeError('creation timed out')
            raise RuntimeError('ownership query unavailable')
        with patch.object(acceptance, 'command', side_effect=command), redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, '^creation timed out$'):
                self.run_main(harness)
        self.assertEqual('creation timed out', harness.report['error'])
        self.assertEqual('query-container', harness.report['cleanupErrors'][0]['action'])
        self.assertFalse(harness.report['passed'])

    def test_cleanup_errors_fail_an_otherwise_successful_run(self):
        harness = Mock()
        harness.report = {}
        harness.cleanup.return_value = [{'error': 'cleanup failure'}]
        with self.assertRaisesRegex(RuntimeError, 'Acceptance cleanup failed'):
            self.run_main(harness)

    def test_etl_only_keeps_its_scope_and_does_not_run_budgets(self):
        harness = Mock()
        harness.report = {'resourceBudgets': acceptance.resource_budget_report(['node1'], 2, 12)}
        harness.cleanup.return_value = []
        self.run_main(harness, '--etl-only', '--max-heap-mib', '2', '--max-threads', '12')
        harness.etl_output.assert_called_once()
        harness.exercise.assert_not_called()
        self.assertEqual('not_run', harness.report['resourceBudgets']['heap']['status'])
        self.assertEqual('not_run', harness.report['resourceBudgets']['threads']['status'])


if __name__ == '__main__':
    unittest.main()
