from contextlib import redirect_stdout
import ast
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shlex
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch
import warnings
from xml.etree import ElementTree
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DemoInitializationTest(unittest.TestCase):
    def test_preserves_existing_key(self):
        with tempfile.TemporaryDirectory() as directory:
            env = load("init-demo").initialize(directory)
            first = env.read_bytes()
            load("init-demo").initialize(directory)
            self.assertEqual(first, env.read_bytes())
            self.assertEqual(0o600, env.stat().st_mode & 0o777)
            self.assertTrue((Path(directory) / "demo/runtime/security").is_dir())

    def test_rejects_invalid_env_without_overwriting(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / ".env"
            env.write_text("OTHER=value\n")
            with self.assertRaises(ValueError):
                load("init-demo").initialize(directory)
            self.assertEqual("OTHER=value\n", env.read_text())


class PublicationChecksTest(unittest.TestCase):
    def test_detects_broken_links_and_forbidden_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "README.md").write_text("[missing](missing.md)\n")
            (root / "private.docx").write_bytes(b"not public")
            errors = load("check-project").check(root)
            self.assertEqual(2, len(errors))

    def test_valid_links_and_synthetic_values(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "README.md").write_text("[guide](guide.md#section)\n[web](https://example.com)\n")
            (root / "guide.md").write_text("# Guide\nDemo password: demo-password\n")
            self.assertEqual([], load("check-project").check(root))

    def test_token_is_detected_without_echoing_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            token = "glpat-" + "x" * 24
            (root / "data.txt").write_text(token)
            errors = load("check-project").check(root)
            self.assertEqual(1, len(errors))
            self.assertNotIn(token, errors[0])


class InventoryTest(unittest.TestCase):
    def test_lists_actual_jars_with_checksums_and_coordinates(self):
        jar = io.BytesIO()
        with zipfile.ZipFile(jar, "w") as stream:
            stream.writestr("META-INF/maven/example/library/pom.properties",
                            "groupId=example\nartifactId=library\nversion=1.2.3\n")
            stream.writestr("META-INF/LICENSE", "example")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.nar"
            with zipfile.ZipFile(path, "w") as stream:
                stream.writestr("META-INF/bundled-dependencies/library-1.2.3.jar", jar.getvalue())
            result = load("nar-inventory").inventory(path)
            self.assertEqual(1, len(result["jars"]))
            row = result["jars"][0]
            self.assertEqual(hashlib.sha256(jar.getvalue()).hexdigest(), row["sha256"])
            self.assertEqual(["META-INF/LICENSE"], row["licenseAndNoticePaths"])
            self.assertEqual("library", row["embeddedMavenCoordinates"][0]["artifactId"])


class AdvisoryCoverageTest(unittest.TestCase):
    def test_inventory_identifies_bouncy_castle_without_maven_metadata(self):
        jar = io.BytesIO()
        with zipfile.ZipFile(jar, "w") as stream:
            stream.writestr("META-INF/LICENSE", "example")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.nar"
            with zipfile.ZipFile(path, "w") as stream:
                stream.writestr("META-INF/bundled-dependencies/bcprov-jdk18on-1.86.jar", jar.getvalue())
            row = load("nar-inventory").inventory(path)["jars"][0]
            self.assertEqual([], row["embeddedMavenCoordinates"])
            coordinate = row["knownFilenameCoordinates"][0]
            self.assertEqual("org.bouncycastle", coordinate["groupId"])
            self.assertEqual("1.86", coordinate["version"])

    def test_advisories_include_inferred_coordinates_and_report_unknown_jars(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inventory.json"
            path.write_text(json.dumps({"jars": [
                {"path": "bc.jar", "embeddedMavenCoordinates": [],
                 "knownFilenameCoordinates": [{"groupId": "org.bouncycastle",
                    "artifactId": "bcprov-jdk18on", "version": "1.86"}]},
                {"path": "unknown.jar", "embeddedMavenCoordinates": []},
                {"path": "own.jar", "embeddedMavenCoordinates": [{"groupId": "example",
                    "artifactId": "own", "version": "1.0-SNAPSHOT"}]}
            ]}))
            packages, unknown = load("check-advisories").collect([path])
            self.assertEqual([("org.bouncycastle:bcprov-jdk18on", "1.86")], packages)
            self.assertEqual([{"inventory": str(path), "jar": "unknown.jar"}], unknown)


class NarVerificationTest(unittest.TestCase):
    PROCESSOR = "io.github.paf91.nifi.opcua.ConsumeOPCUA"
    RETIRED = "io.github.paf91.nifi.opcua.SubscribeOpcUa"
    SERVICE = "META-INF/services/org.apache.nifi.processor.Processor"
    MANIFEST = "META-INF/docs/extension-manifest.xml"
    JAR_METADATA = "META-INF/maven/io.github.paf91.nifi/nifi-opcua-processors/pom.properties"
    VERSION = "1.0.5-SNAPSHOT"

    def build_nar(self, path, major=52, *, services=None, extensions=None,
                  missing_jar_entries=(), missing_nar_entries=(), manifest=None,
                  jar_overrides=None, nar_overrides=None, extra_jar_entries=None,
                  processor_jar="nifi-opcua-processors-1.0.5-SNAPSHOT.jar",
                  extra_jar="extra-fixture-1.0.jar", duplicate_jar_entries=(),
                  duplicate_extra_jar_entries=(), duplicate_nar_entries=()):
        if services is None:
            services = "# OPC UA processor\n\n" + self.PROCESSOR + " # canonical\n"
        if manifest is None:
            root = ElementTree.Element("extensionManifest")
            for name, value in (("groupId", "io.github.paf91.nifi"),
                                ("artifactId", "nifi-opcua-nar"), ("version", self.VERSION)):
                ElementTree.SubElement(root, name).text = value
            entries = ElementTree.SubElement(root, "extensions")
            if extensions is None:
                extensions = [(self.PROCESSOR, "PROCESSOR")]
            for name, kind in extensions:
                entry = ElementTree.SubElement(entries, "extension")
                ElementTree.SubElement(entry, "name").text = name
                ElementTree.SubElement(entry, "type").text = kind
            manifest = ElementTree.tostring(root)
        jar_entries = {
            self.SERVICE: services, "META-INF/LICENSE": "test license",
            self.JAR_METADATA: ("groupId=io.github.paf91.nifi\nartifactId=nifi-opcua-processors\n"
                                f"version={self.VERSION}\n"),
            self.PROCESSOR.replace(".", "/") + ".class": (
                b"\xca\xfe\xba\xbe\x00\x00" + major.to_bytes(2, "big"))}
        nar_entries = {
            self.MANIFEST: manifest, "META-INF/LICENSE": "test license",
            "META-INF/MANIFEST.MF": ("Manifest-Version: 1.0\r\nNar-Group: io.github.paf91.nifi\r\n"
                                     "Nar-Id: nifi-opcua-nar\r\n"
                                     f"Nar-Version: {self.VERSION}\r\n\r\n")}
        for suffix in ("html", "md"):
            jar_entries[f"docs/{self.PROCESSOR}/additionalDetails.{suffix}"] = "Processor details"
            nar_entries[f"META-INF/docs/additional-details/{self.PROCESSOR}/additionalDetails.{suffix}"] = (
                "Processor details")
        jar_entries.update(jar_overrides or {})
        nar_entries.update(nar_overrides or {})
        jar = io.BytesIO()
        with zipfile.ZipFile(jar, "w") as stream:
            for name, content in jar_entries.items():
                if name not in missing_jar_entries:
                    stream.writestr(name, content)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                for name, content in duplicate_jar_entries:
                    stream.writestr(name, content)
        nar_entries["META-INF/bundled-dependencies/" + processor_jar] = jar.getvalue()
        if extra_jar_entries is not None:
            extra = io.BytesIO()
            with zipfile.ZipFile(extra, "w") as stream:
                for name, content in extra_jar_entries.items():
                    stream.writestr(name, content)
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    for name, content in duplicate_extra_jar_entries:
                        stream.writestr(name, content)
            nar_entries["META-INF/bundled-dependencies/" + extra_jar] = extra.getvalue()
        with zipfile.ZipFile(path, "w") as stream:
            for name, content in nar_entries.items():
                if name not in missing_nar_entries:
                    stream.writestr(name, content)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                for name, content in duplicate_nar_entries:
                    stream.writestr(name, content)

    def test_accepts_canonical_processor_with_target_bytecode_and_commented_service(self):
        for major in (52, 65):
            with self.subTest(major=major), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, major)
                self.assertEqual((1, 1), load("verify-nar").verify(path, major))

    def test_accepts_an_unrelated_bundled_jar(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.nar"
            self.build_nar(path, extra_jar_entries={"META-INF/LICENSE": "dependency license"})
            self.assertEqual((2, 1), load("verify-nar").verify(path, 52))

    def test_accepts_unrelated_services_and_empty_processor_services_in_dependencies(self):
        payloads = [{"META-INF/services/example.OtherService": "example.OtherProvider\n"},
                    {self.SERVICE: ""}, {self.SERVICE: " \n\t# no processor providers\n  \n"}]
        for payload in payloads:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, extra_jar_entries=payload)
                self.assertEqual((2, 1), load("verify-nar").verify(path, 52))

    def test_accepts_canonical_inner_classes_in_dependencies(self):
        helper = self.PROCESSOR.replace(".", "/") + "$Helper.class"
        bytecode = b"\xca\xfe\xba\xbe\x00\x00\x00\x34"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.nar"
            self.build_nar(path, extra_jar_entries={
                helper: bytecode, "META-INF/versions/21/" + helper: bytecode})
            self.assertEqual((2, 2), load("verify-nar").verify(path, 52))

    def test_rejects_missing_service_or_extension(self):
        for options in ({"services": ""}, {"extensions": []}):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, **options)
                with self.assertRaises(ValueError):
                    load("verify-nar").verify(path, 52)

    def test_rejects_each_missing_processor_class_or_details_file(self):
        variants = {"class": {"missing_jar_entries": (
            self.PROCESSOR.replace(".", "/") + ".class",)}}
        for suffix in ("html", "md"):
            variants["jar-" + suffix] = {"missing_jar_entries": (
                f"docs/{self.PROCESSOR}/additionalDetails.{suffix}",)}
            variants["nar-" + suffix] = {"missing_nar_entries": (
                f"META-INF/docs/additional-details/{self.PROCESSOR}/additionalDetails.{suffix}",)}
        for artifact, options in variants.items():
            with self.subTest(artifact=artifact), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, **options)
                with self.assertRaises(ValueError):
                    load("verify-nar").verify(path, 52)

    def test_rejects_duplicate_canonical_class_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.nar"
            self.build_nar(path, duplicate_jar_entries=((self.PROCESSOR.replace(".", "/") + ".class",
                                                       b"\xca\xfe\xba\xbe\x00\x00\x00\x34"),))
            with self.assertRaises(ValueError):
                load("verify-nar").verify(path, 52)

    def test_rejects_canonical_class_in_an_extra_bundled_jar(self):
        canonical = self.PROCESSOR.replace(".", "/") + ".class"
        for entry in (canonical, "META-INF/versions/21/" + canonical):
            with self.subTest(entry=entry), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, extra_jar_entries={entry: b"\xca\xfe\xba\xbe\x00\x00\x00\x34"})
                with self.assertRaises(ValueError):
                    load("verify-nar").verify(path, 52)

    def test_rejects_multi_release_canonical_class_in_processor_jar(self):
        entry = "META-INF/versions/21/" + self.PROCESSOR.replace(".", "/") + ".class"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.nar"
            self.build_nar(path, jar_overrides={entry: b"\xca\xfe\xba\xbe\x00\x00\x00\x41"})
            with self.assertRaises(ValueError):
                load("verify-nar").verify(path, 52)

    def test_rejects_main_jar_missing_canonical_class_even_when_a_dependency_has_it(self):
        canonical = self.PROCESSOR.replace(".", "/") + ".class"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.nar"
            self.build_nar(path, missing_jar_entries=(canonical,),
                           extra_jar_entries={canonical: b"\xca\xfe\xba\xbe\x00\x00\x00\x34"})
            with self.assertRaises(ValueError):
                load("verify-nar").verify(path, 52)

    def test_rejects_missing_service_or_manifest_file(self):
        for options in ({"missing_jar_entries": (self.SERVICE,)},
                        {"missing_nar_entries": (self.MANIFEST,)},
                        {"missing_nar_entries": ("META-INF/MANIFEST.MF",)},
                        {"missing_jar_entries": (self.JAR_METADATA,)}):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, **options)
                with self.assertRaises((KeyError, ValueError)):
                    load("verify-nar").verify(path, 52)

    def test_rejects_duplicate_or_unexpected_service_and_extension_entries(self):
        for extra in (self.PROCESSOR, self.RETIRED, "example.UnexpectedProcessor"):
            names = (self.PROCESSOR, extra)
            variants = {"service": {"services": "\n".join(names)},
                        "extension": {"extensions": [(name, "PROCESSOR") for name in names]}}
            for artifact, options in variants.items():
                with self.subTest(extra=extra, artifact=artifact), tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "test.nar"
                    self.build_nar(path, **options)
                    with self.assertRaises(ValueError):
                        load("verify-nar").verify(path, 52)

    def test_rejects_processor_service_providers_in_an_extra_bundled_jar(self):
        for provider in (self.PROCESSOR, "example.UnexpectedProcessor"):
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, extra_jar_entries={self.SERVICE: provider + " # extra provider\n"})
                with self.assertRaises(ValueError):
                    load("verify-nar").verify(path, 52)

    def test_rejects_main_jar_missing_service_even_when_a_dependency_provides_it(self):
        for options in ({"missing_jar_entries": (self.SERVICE,)}, {"services": "# no providers\n"}):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, extra_jar_entries={self.SERVICE: self.PROCESSOR + "\n"}, **options)
                with self.assertRaises((KeyError, ValueError)):
                    load("verify-nar").verify(path, 52)

    def test_rejects_duplicate_processor_service_files_in_each_archive(self):
        contents = [("canonical", self.PROCESSOR + "\n", self.PROCESSOR + "\n"),
                    ("retired first", self.RETIRED + "\n", self.PROCESSOR + "\n"),
                    ("empty", "# no providers\n", " \n\t# still no providers\n")]
        for archive in ("processor", "dependency", "nar"):
            for case, first, last in contents:
                duplicate = ((self.SERVICE, last),)
                variants = {
                    "processor": {"services": first, "duplicate_jar_entries": duplicate},
                    "dependency": {"extra_jar_entries": {self.SERVICE: first},
                                   "duplicate_extra_jar_entries": duplicate},
                    "nar": {"nar_overrides": {self.SERVICE: first}, "duplicate_nar_entries": duplicate}}
                with self.subTest(archive=archive, case=case), tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "test.nar"
                    self.build_nar(path, **variants[archive])
                    with self.assertRaises(ValueError):
                        load("verify-nar").verify(path, 52)

    def test_rejects_nonprocessor_extension_types(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.nar"
            self.build_nar(path, extensions=[(self.PROCESSOR, "CONTROLLER_SERVICE")])
            with self.assertRaisesRegex(ValueError, "PROCESSOR"):
                load("verify-nar").verify(path, 52)

    def test_alternative_reference_does_not_register_an_extension(self):
        manifest = ("<extensionManifest><groupId>io.github.paf91.nifi</groupId>"
                    "<artifactId>nifi-opcua-nar</artifactId>"
                    f"<version>{self.VERSION}</version><extensions>"
                    "<extension><name>example.UnrelatedProcessor</name>"
                    f"<type>PROCESSOR</type><deprecationNotice><alternatives>"
                    f"<alternative>{self.PROCESSOR}</alternative></alternatives></deprecationNotice>"
                    "</extension></extensions></extensionManifest>")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.nar"
            self.build_nar(path, manifest=manifest)
            with self.assertRaises(ValueError):
                load("verify-nar").verify(path, 52)

    def test_rejects_retired_classes_docs_and_providers_in_any_bundled_jar(self):
        retired_path = self.RETIRED.replace(".", "/")
        bytecode = b"\xca\xfe\xba\xbe\x00\x00\x00\x34"
        payloads = [
            {retired_path + ".class": bytecode},
            {retired_path + "$Helper.class": bytecode},
            {"META-INF/versions/21/" + retired_path + "$Helper.class": bytecode},
            {self.SERVICE: self.RETIRED + " # retired\n"},
            {self.SERVICE: self.RETIRED + "$Helper\n"}]
        for suffix in ("html", "md"):
            payloads.append({f"docs/{self.RETIRED}/additionalDetails.{suffix}": "retired details"})
        for payload in payloads:
            for location in ("processor", "dependency"):
                options = ({"jar_overrides": payload} if location == "processor"
                           else {"extra_jar_entries": payload})
                with self.subTest(payload=list(payload), location=location), tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "test.nar"
                    self.build_nar(path, **options)
                    with self.assertRaises(ValueError):
                        load("verify-nar").verify(path, 52)

    def test_rejects_retired_classes_and_details_at_nar_level(self):
        retired_path = self.RETIRED.replace(".", "/")
        bytecode = b"\xca\xfe\xba\xbe\x00\x00\x00\x34"
        payloads = [{retired_path + ".class": bytecode},
                    {"META-INF/versions/21/" + retired_path + "$Helper.class": bytecode}]
        for suffix in ("html", "md"):
            payloads.append({f"META-INF/docs/additional-details/{self.RETIRED}/additionalDetails.{suffix}":
                             "retired details"})
        for payload in payloads:
            with self.subTest(payload=list(payload)), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, nar_overrides=payload)
                with self.assertRaises(ValueError):
                    load("verify-nar").verify(path, 52)

    def test_rejects_retired_processor_jar_name(self):
        retired = "nifi-opcua-subscriber-processors-1.0.5-SNAPSHOT.jar"
        for options in ({"processor_jar": retired},
                        {"extra_jar_entries": {}, "extra_jar": retired}):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, **options)
                with self.assertRaises(ValueError):
                    load("verify-nar").verify(path, 52)

    def test_rejects_wrong_bundle_and_processor_coordinates(self):
        variants = []
        manifest = ("Manifest-Version: 1.0\nNar-Group: io.github.paf91.nifi\n"
                    f"Nar-Id: nifi-opcua-nar\nNar-Version: {self.VERSION}\n\n")
        for original, replacement in (("io.github.paf91.nifi", "example.wrong"),
                                       ("nifi-opcua-nar", "nifi-opcua-subscriber-nar"),
                                       (self.VERSION, "")):
            variants.append({"nar_overrides": {"META-INF/MANIFEST.MF": manifest.replace(original, replacement)}})
        for field, wrong in (("groupId", "example.wrong"),
                             ("artifactId", "nifi-opcua-subscriber-nar"),
                             ("version", "0.0.0")):
            root = ElementTree.fromstring(
                "<extensionManifest><groupId>io.github.paf91.nifi</groupId>"
                f"<artifactId>nifi-opcua-nar</artifactId><version>{self.VERSION}</version>"
                f"<extensions><extension><name>{self.PROCESSOR}</name>"
                "<type>PROCESSOR</type></extension></extensions></extensionManifest>")
            root.find(field).text = wrong
            variants.append({"manifest": ElementTree.tostring(root)})
        metadata = ("groupId=io.github.paf91.nifi\nartifactId=nifi-opcua-processors\n"
                    f"version={self.VERSION}\n")
        for original, replacement in (("io.github.paf91.nifi", "example.wrong"),
                                       ("nifi-opcua-processors", "nifi-opcua-subscriber-processors"),
                                       (self.VERSION, "0.0.0")):
            variants.append({"jar_overrides": {self.JAR_METADATA: metadata.replace(original, replacement)}})
        for index, options in enumerate(variants):
            with self.subTest(variant=index), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, **options)
                with self.assertRaises(ValueError):
                    load("verify-nar").verify(path, 52)

    def test_rejects_missing_project_licenses(self):
        for options in ({"missing_jar_entries": ("META-INF/LICENSE",)},
                        {"missing_nar_entries": ("META-INF/LICENSE",)}):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "test.nar"
                self.build_nar(path, **options)
                with self.assertRaisesRegex(ValueError, "license"):
                    load("verify-nar").verify(path, 52)

    def test_rejects_wrong_jvm_target(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.nar"
            self.build_nar(path, 65)
            with self.assertRaisesRegex(ValueError, "Incompatible bytecode"):
                load("verify-nar").verify(path, 52)


class ReleaseTest(unittest.TestCase):
    def test_target_artifacts_follow_pom_versions(self):
        fixtures = [('''<project xmlns="http://maven.apache.org/POM/4.0.0">
            <version>7.4.1-SNAPSHOT</version><profiles>
            <profile><id>nifi1</id><properties><nifi.version>1.28.3</nifi.version>
            <maven.compiler.release>8</maven.compiler.release></properties></profile>
            <profile><id>unrelated</id></profile>
            <profile><id>nifi2</id><properties><nifi.version>2.19.5</nifi.version>
            <maven.compiler.release>21</maven.compiler.release></properties></profile>
            </profiles></project>''', [
                {'profile': 'nifi1', 'java': '8', 'nifi': '1.28.3', 'bytecode': '52',
                 'artifact': 'nifi-opcua-1.28.3-7.4.1-SNAPSHOT.nar'},
                {'profile': 'nifi2', 'java': '21', 'nifi': '2.19.5', 'bytecode': '65',
                 'artifact': 'nifi-opcua-2.19.5-7.4.1-SNAPSHOT.nar'}]),
            ('''<project xmlns="http://maven.apache.org/POM/4.0.0">
            <version>9.6.2</version><profiles>
            <profile><id>nifi2</id><properties><nifi.version>2.31.7</nifi.version>
            <maven.compiler.release>25</maven.compiler.release></properties></profile>
            <profile><id>nifi1</id><properties><nifi.version>1.35.9</nifi.version>
            <maven.compiler.release>11</maven.compiler.release></properties></profile>
            </profiles></project>''', [
                {'profile': 'nifi2', 'java': '25', 'nifi': '2.31.7', 'bytecode': '69',
                 'artifact': 'nifi-opcua-2.31.7-9.6.2.nar'},
                {'profile': 'nifi1', 'java': '11', 'nifi': '1.35.9', 'bytecode': '55',
                 'artifact': 'nifi-opcua-1.35.9-9.6.2.nar'}])]
        for pom, expected in fixtures:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'pom.xml'
                path.write_text(pom)
                self.assertEqual(expected, load('release').build_targets(path))

    def test_target_artifacts_require_both_profiles(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'pom.xml'
            path.write_text('<project xmlns="http://maven.apache.org/POM/4.0.0"><version>3.2.1</version></project>')
            with self.assertRaisesRegex(ValueError, 'Both build profiles'):
                load('release').build_targets(path)

    def test_manifest_covers_nested_files_without_hashing_itself(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "nested").mkdir()
            (root / "nested/artifact.nar").write_bytes(b"artifact")
            (root / "SHA256SUMS").write_text("old manifest")
            load("release").manifest(root)
            self.assertEqual(hashlib.sha256(b"artifact").hexdigest() + "  nested/artifact.nar\n",
                             (root / "SHA256SUMS").read_text())


class MavenCiConfigurationTest(unittest.TestCase):
    SETTINGS = "scripts/maven-ci-settings.xml"

    def test_mirror_targets_only_the_two_redundant_release_repositories(self):
        root = ElementTree.parse(ROOT / self.SETTINGS).getroot()
        ns = {"s": "http://maven.apache.org/SETTINGS/1.2.0"}
        mirrors = root.findall("s:mirrors/s:mirror", ns)
        self.assertEqual(1, len(mirrors))
        mirror = mirrors[0]
        self.assertEqual("https://repo.maven.apache.org/maven2", mirror.findtext("s:url", namespaces=ns))
        repositories = mirror.findtext("s:mirrorOf", namespaces=ns).split(",")
        self.assertEqual({"dash-licenses-releases", "rso-public-grid"}, set(repositories))
        self.assertEqual(2, len(repositories))
        self.assertEqual([], root.findall("s:servers", ns))
        self.assertEqual([], root.findall("s:profiles", ns))

    def test_every_owned_ci_maven_command_uses_the_shared_settings(self):
        commands = []
        for path in (ROOT / ".github/workflows/ci.yml", ROOT / ".gitlab-ci.yml"):
            source = path.read_text().replace("\\\n", " ")
            found = re.findall(r"\bmvn\s+[^\n]+", source)
            self.assertTrue(found, path)
            for command in found:
                with self.subTest(path=path, command=command):
                    arguments = shlex.split(command)
                    self.assertIn("-s", arguments)
                    self.assertEqual(self.SETTINGS, arguments[arguments.index("-s") + 1])
                    commands.append(arguments)
        self.assertTrue(any("demo/pom.xml" in command for command in commands))
        self.assertTrue(any("verify" in command for command in commands))

    def test_release_maven_calls_use_settings_from_the_exported_source(self):
        tree = ast.parse((ROOT / "scripts/release.py").read_text())
        commands = [node for node in ast.walk(tree) if isinstance(node, ast.List)
                    and node.elts and isinstance(node.elts[0], ast.Constant)
                    and node.elts[0].value == "mvn"]
        self.assertEqual(2, len(commands))
        for command in commands:
            arguments = [node.value for node in command.elts if isinstance(node, ast.Constant)]
            self.assertEqual(["mvn", "-s", self.SETTINGS], arguments[:3])
        self.assertFalse(load("check-project").forbidden(self.SETTINGS))
        self.assertTrue((ROOT / self.SETTINGS).is_file())

    @staticmethod
    def blocks(source, indentation):
        matches = list(re.finditer(r"^" + " " * indentation + r"([.\w-]+):[^\n]*$",
                                   source, re.MULTILINE))
        return {match.group(1): source[match.end():
                    matches[index + 1].start() if index + 1 < len(matches) else len(source)]
                for index, match in enumerate(matches)}

    def test_all_ci_jobs_have_a_bounded_timeout_including_inherited_java25(self):
        github = (ROOT / ".github/workflows/ci.yml").read_text().split("jobs:\n", 1)[1]
        jobs = self.blocks(github, 2)
        self.assertEqual({"repository", "build"}, set(jobs))
        for name, block in jobs.items():
            with self.subTest(provider="github", job=name):
                timeout = re.search(r"^    timeout-minutes: (\d+)\s*$", block, re.MULTILINE)
                self.assertIsNotNone(timeout)
                self.assertGreater(int(timeout.group(1)), 0)
                self.assertLessEqual(int(timeout.group(1)), 60)
        gitlab = self.blocks((ROOT / ".gitlab-ci.yml").read_text(), 0)
        for name in ("repository", "open62541", "nifi1-java8", "nifi2-java21", "nifi2-java25"):
            with self.subTest(provider="gitlab", job=name):
                inherited = name
                seen = set()
                while True:
                    self.assertNotIn(inherited, seen, "Cyclic GitLab job inheritance")
                    seen.add(inherited)
                    block = gitlab[inherited]
                    timeout = re.search(r"^  timeout: (\d+)m\s*$", block, re.MULTILINE)
                    if timeout:
                        self.assertGreater(int(timeout.group(1)), 0)
                        self.assertLessEqual(int(timeout.group(1)), 60)
                        break
                    parent = re.search(r"^  extends: ([.\w-]+)\s*$", block, re.MULTILINE)
                    self.assertIsNotNone(parent)
                    inherited = parent.group(1)
        self.assertIn("nifi2-java21", gitlab["nifi2-java25"])


class FormatterCiConfigurationTest(unittest.TestCase):
    CACHE_PATH = '.tools/google-java-format-*.jar'

    @staticmethod
    def sections(source, indentation):
        matches = list(re.finditer(r'^' + ' ' * indentation + r'([.\w-]+):([^\n]*)$',
                                   source, re.MULTILINE))
        return {match.group(1): (match.group(2).strip(), source[match.end():
                    matches[index + 1].start() if index + 1 < len(matches) else len(source)])
                for index, match in enumerate(matches)}

    @staticmethod
    def scalar(value):
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            return value[1:-1]
        return value

    def sequence(self, section, indentation):
        value, body = section
        if value:
            self.assertTrue(value.startswith('[') and value.endswith(']'), value)
            return [self.scalar(item.strip()) for item in value[1:-1].split(',') if item.strip()]
        return [self.scalar(item) for item in re.findall(
            r'^' + ' ' * indentation + r'-\s+([^\n]+?)\s*$', body, re.MULTILINE)]

    def test_github_formatter_cache_uses_the_script_hash_and_narrow_path(self):
        source = (ROOT / '.github/workflows/ci.yml').read_text()
        top = self.sections(source, 0)
        permissions = self.sections(top['permissions'][1], 2)
        self.assertEqual({'contents'}, set(permissions))
        self.assertEqual('read', permissions['contents'][0])
        jobs = self.sections(top['jobs'][1], 2)
        steps = re.split(r'^      - ', jobs['repository'][1], flags=re.MULTILINE)[1:]
        caches = [step for step in steps if re.search(
            r'^(?:uses:|        uses:) actions/cache(?:/|@)', step, re.MULTILINE)]
        self.assertEqual(1, len(caches))
        cache = caches[0]
        action = re.search(r'^(?:uses:|        uses:) (\S+)', cache, re.MULTILINE).group(1)
        self.assertEqual('actions/cache@0057852bfaa89a56745cba8c7296529d2fc39830', action)
        options = self.sections(self.sections(cache, 8)['with'][1], 10)
        self.assertEqual({'path', 'key'}, set(options))
        self.assertEqual(self.CACHE_PATH, self.scalar(options['path'][0]))
        self.assertEqual("${{ runner.os }}-google-java-format-${{ hashFiles('scripts/format-java.py') }}",
                         self.scalar(options['key'][0]))
        checks = [step for step in steps if re.search(
            r'^(?:run:|        run:) python3 scripts/format-java.py --check\s*$',
            step, re.MULTILINE)]
        self.assertEqual(1, len(checks))
        self.assertNotIn('if', self.sections(checks[0], 8))
        self.assertFalse(checks[0].startswith('if:'))
        self.assertLess(steps.index(cache), steps.index(checks[0]))
        self.assertNotRegex(jobs['build'][1], r'\bactions/cache(?:/|@)')

    def test_gitlab_formatter_cache_is_saved_only_on_success_without_fallbacks(self):
        source = (ROOT / '.gitlab-ci.yml').read_text()
        jobs = self.sections(source, 0)
        repository = self.sections(jobs['repository'][1], 2)
        cache = self.sections(repository['cache'][1], 4)
        self.assertEqual({'key', 'paths', 'when'}, set(cache))
        self.assertEqual('on_success', cache['when'][0])
        self.assertEqual([self.CACHE_PATH], self.sequence(cache['paths'], 6))
        key = self.sections(cache['key'][1], 6)
        self.assertEqual({'files', 'prefix'}, set(key))
        self.assertEqual(['scripts/format-java.py'], self.sequence(key['files'], 8))
        self.assertEqual('google-java-format', self.scalar(key['prefix'][0]))
        commands = self.sequence(repository['script'], 4)
        self.assertEqual(1, commands.count('python3 scripts/format-java.py --check'))
        self.assertNotIn('rules', repository)
        self.assertNotIn('extends', repository)

    def test_gitlab_maven_and_fixture_caches_keep_their_existing_scope(self):
        jobs = self.sections((ROOT / '.gitlab-ci.yml').read_text(), 0)
        shared = self.sections(jobs['cache'][1], 2)
        self.assertEqual({'key', 'paths'}, set(shared))
        self.assertEqual('maven-$CI_JOB_NAME', self.scalar(shared['key'][0]))
        self.assertEqual(['.m2/repository/'], self.sequence(shared['paths'], 4))
        fixture = self.sections(jobs['open62541'][1], 2)
        self.assertEqual([], self.sequence(fixture['cache'], 4))
        for name in ('.build', 'nifi1-java8', 'nifi2-java21', 'nifi2-java25'):
            with self.subTest(job=name):
                self.assertNotIn('cache', self.sections(jobs[name][1], 2))


class InteropCoverageTest(unittest.TestCase):
    SHARED = ('Open62541Test', 'Open62541RecoveryTest')
    MODERN = ('Open62541Test', 'Open62541RecoveryTest', 'Open62541SetupTest')

    def reports(self, root, names):
        for name in names:
            (root / ('TEST-io.github.paf91.nifi.opcua.' + name + '.xml')).write_text(
                '<testsuite><testcase name="live"/></testsuite>')

    def test_accepts_successful_independent_suites(self):
        for profile, names, count in (('nifi1', self.SHARED, 2), ('nifi2', self.MODERN, 3)):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.reports(root, names)
                self.assertEqual(count, load('verify-interop-tests').verify(root, profile))

    def test_rejects_each_missing_required_report(self):
        for profile, names in (('nifi1', self.SHARED), ('nifi2', self.MODERN)):
            for missing in names:
                with self.subTest(profile=profile, missing=missing), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    self.reports(root, [name for name in names if name != missing])
                    with self.assertRaisesRegex(ValueError, 'Missing.*' + missing):
                        load('verify-interop-tests').verify(root, profile)

    def test_rejects_skips_failures_errors_and_empty_suites(self):
        for profile, names in (('nifi1', self.SHARED), ('nifi2', self.MODERN)):
            for name in names:
                for case in ('', '<testcase><skipped/></testcase>',
                             '<testcase><failure/></testcase>', '<testcase><error/></testcase>'):
                    with self.subTest(profile=profile, name=name, case=case), tempfile.TemporaryDirectory() as directory:
                        root = Path(directory)
                        self.reports(root, names)
                        (root / ('TEST-io.github.paf91.nifi.opcua.' + name + '.xml')).write_text(
                            '<testsuite>' + case + '</testsuite>')
                        with self.assertRaisesRegex(ValueError, 'without skips.*' + name):
                            load('verify-interop-tests').verify(root, profile)

    def test_cli_requires_profile(self):
        module = load('verify-interop-tests')
        with patch('sys.argv', ['verify-interop-tests.py', '/tmp/reports']), patch('sys.stderr', io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                module.main()
        self.assertEqual(2, error.exception.code)


class AcceptanceTimestampTest(unittest.TestCase):
    def test_java_nanoseconds_and_milliseconds_are_accepted(self):
        parse = load("acceptance").timestamp
        self.assertAlmostEqual(parse("2026-01-01T00:00:00.123456789Z"),
                               parse("2026-01-01T00:00:00.123456Z"))
        self.assertEqual(parse("2026-01-01T00:00:00.123Z"),
                         parse("2026-01-01T00:00:00.123000Z"))


class AcceptanceResourcesTest(unittest.TestCase):
    def test_cluster_samples_use_node_values_not_aggregate(self):
        diagnostics = {"aggregateSnapshot": {"usedHeapBytes": 300}, "nodeSnapshots": [
            {"address": "node1", "snapshot": {"usedHeapBytes": 100, "totalThreads": 10}},
            {"address": "node2", "snapshot": {"usedHeapBytes": 200, "totalThreads": 20}}]}
        result = load("acceptance").resource_snapshots(diagnostics, "node1")
        self.assertEqual(100, result["node1"]["usedHeapBytes"])
        self.assertEqual(200, result["node2"]["usedHeapBytes"])

    def test_standalone_uses_its_only_snapshot(self):
        result = load("acceptance").resource_snapshots({"aggregateSnapshot": {"totalThreads": 8}}, "node1")
        self.assertEqual(8, result["node1"]["totalThreads"])


class AcceptanceProcessorSelectionTest(unittest.TestCase):
    def test_flow_selects_exact_canonical_class_and_bundle(self):
        acceptance = load("acceptance")
        canonical = "io.github.paf91.nifi.opcua.ConsumeOPCUA"
        bundle = {"group": "io.github.paf91.nifi", "artifact": "nifi-opcua-nar",
                  "version": "1.0.5-SNAPSHOT"}
        old_bundle = dict(bundle, artifact="nifi-opcua-subscriber-nar")
        wrong_group = dict(bundle, group="example.wrong")
        harness = acceptance.Harness.__new__(acceptance.Harness)
        harness.node = "node1"
        harness.args = SimpleNamespace(tags=2)
        harness.report = {}
        created = []

        def api(node, method, path, body=None):
            if path == "/flow/process-groups/root":
                return {"processGroupFlow": {"id": "root"}}
            if method == "POST" and path == "/process-groups/root/process-groups":
                return {"id": "group"}
            if path == "/flow/processor-types":
                return {"processorTypes": [
                    {"type": "example.ConsumeOPCUA", "bundle": bundle},
                    {"type": canonical, "bundle": old_bundle},
                    {"type": canonical, "bundle": wrong_group},
                    {"type": canonical, "bundle": bundle},
                    {"type": "org.apache.nifi.processors.standard.LogAttribute",
                     "bundle": {"artifact": "nifi-standard-nar"}}]}
            if method == "POST" and path == "/process-groups/group/processors":
                created.append(body["component"])
                return {"id": "source" if len(created) == 1 else "sink"}
            if method == "POST" and path == "/process-groups/group/connections":
                return {"id": "connection"}
            if method == "GET" and path in ("/processors/source", "/processors/sink"):
                return {"component": {"validationStatus": "VALID"}}
            self.fail(f"Unexpected API request: {method} {path}")

        harness.api = Mock(side_effect=api)
        harness.wait = Mock(side_effect=lambda description, predicate: predicate())
        harness.state = Mock()
        harness.queued = Mock(return_value=1)
        harness.inspect_content = Mock()
        harness.metrics = Mock(return_value={"subscriptions": 1, "monitoredItems": 2})
        harness.check = Mock()
        harness.flow()
        self.assertEqual(canonical, created[0]["type"])
        self.assertEqual(bundle, created[0]["bundle"])
        self.assertEqual("ConsumeOPCUA", harness.report["processorType"])
        self.assertEqual("opc.tcp://opcua:4840/demo",
                         created[0]["config"]["properties"]["Endpoint URL"])
        self.assertEqual("Tag0 | ns=2;s=Load.Tag0\nTag1 | ns=2;s=Load.Tag1",
                         created[0]["config"]["properties"]["Tags"])
        self.assertEqual([call("source", "RUNNING"), call("sink", "RUNNING")],
                         harness.state.call_args_list)


class SourceExportTest(unittest.TestCase):
    def test_archive_prefix_and_entry_checks_use_the_new_project_name(self):
        exporter = load("export-public")
        packed = io.BytesIO()
        with tarfile.open(fileobj=packed, mode="w:gz") as archive:
            payload = b"Synthetic source snapshot"
            entry = tarfile.TarInfo("nifi-opcua/README.md")
            entry.size = len(payload)
            archive.addfile(entry, io.BytesIO(payload))
        checker = SimpleNamespace(check=Mock(return_value=[]), forbidden=Mock(return_value=False))
        commit = "a" * 40
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "source.tar.gz"
            with patch("sys.argv", ["export-public.py", "--output", str(output)]), \
                    patch.object(exporter.importlib.util, "spec_from_file_location", return_value=Mock()), \
                    patch.object(exporter.importlib.util, "module_from_spec", return_value=checker), \
                    patch.object(exporter.subprocess, "check_output",
                                 side_effect=["", commit, packed.getvalue()]) as commands, \
                    redirect_stdout(io.StringIO()):
                exporter.main()
            self.assertEqual(packed.getvalue(), output.read_bytes())
            self.assertEqual(["git", "archive", "--format=tar.gz", "--prefix=nifi-opcua/", commit],
                             commands.call_args_list[2].args[0])
            checker.forbidden.assert_called_once_with("README.md")
            metadata = json.loads(output.with_suffix(".gz.json").read_text())
            self.assertEqual(commit, metadata["sourceCommit"])
            self.assertFalse(metadata["includesGitHistory"])
