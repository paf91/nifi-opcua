#!/usr/bin/env python3
"""Verify bundle identity, registration, docs, licenses and base bytecode."""
import argparse
import io
from pathlib import Path
import re
import struct
from xml.etree import ElementTree
import zipfile

GROUP = "io.github.paf91.nifi"
MAVEN_TARGETS = {52: ("nifi-opcua-nar", "nifi-opcua-processors"),
                 65: ("nifi-opcua-nifi2-nar", "nifi-opcua-nifi2-processors")}
PROCESSOR = "io.github.paf91.nifi.opcua.ConsumeOPCUA"
RETIRED_PROCESSOR = "io.github.paf91.nifi.opcua.SubscribeOpcUa"
SERVICE = "META-INF/services/org.apache.nifi.processor.Processor"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify_maven_coordinates(archive, artifact, version):
    prefix = f"META-INF/maven/{GROUP}/"
    expected = f"{prefix}{artifact}/pom.properties"
    descriptors = [name for name in archive.namelist()
                   if name.startswith(prefix) and name.endswith("/pom.properties")]
    require(descriptors == [expected], "Unexpected Maven descriptor for " + artifact)
    properties = {}
    for line in archive.read(expected).decode().splitlines():
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            properties[key.strip()] = value.strip()
    require(all(properties.get(key) == value for key, value in
                (("groupId", GROUP), ("artifactId", artifact), ("version", version))),
            "Unexpected Maven coordinates for " + artifact)


def service_providers(archive):
    require(archive.namelist().count(SERVICE) == 1, "Expected exactly one Processor service descriptor")
    providers = [line.split("#", 1)[0].strip() for line in archive.read(SERVICE).decode().splitlines()]
    return [provider for provider in providers if provider]


def reject_retired_content(archive):
    retired_class = r"(?:^|/)" + re.escape(RETIRED_PROCESSOR.replace(".", "/")) + r"(?:\$[^/]+)?\.class$"
    retired_docs = (f"docs/{RETIRED_PROCESSOR}", f"META-INF/docs/additional-details/{RETIRED_PROCESSOR}")
    for entry in archive.namelist():
        require(not re.search(retired_class, entry), "Retired processor class is present: " + entry)
        require(not any(entry.rstrip("/") == path or entry.startswith(path + "/") for path in retired_docs),
                "Retired processor documentation is present: " + entry)
    if SERVICE in archive.namelist():
        require(not any(provider == RETIRED_PROCESSOR or provider.startswith(RETIRED_PROCESSOR + "$")
                        for provider in service_providers(archive)), "Retired processor service registration is present")


def verify(path, maximum):
    nar_artifact, processor_artifact = MAVEN_TARGETS[maximum]
    processor_ids = [processor_id for _, processor_id in MAVEN_TARGETS.values()]
    classes = 0
    processors = 0
    processor_classes = 0
    processor_class_pattern = r"(?:META-INF/versions/[0-9]+/)?" + re.escape(PROCESSOR.replace(".", "/")) + r"\.class"
    providers = []
    with zipfile.ZipFile(path) as nar:
        reject_retired_content(nar)
        manifest_text = nar.read("META-INF/MANIFEST.MF").decode().replace("\r\n", "\n")
        manifest = dict(line.split(": ", 1) for line in
                        manifest_text.split("\n\n", 1)[0].replace("\n ", "").splitlines() if ": " in line)
        require(manifest.get("Nar-Group") == GROUP and manifest.get("Nar-Id") == "nifi-opcua-nar",
                "Unexpected NAR bundle coordinates")
        version = manifest.get("Nar-Version")
        require(version is not None and bool(version.strip()), "NAR bundle version is missing")
        verify_maven_coordinates(nar, nar_artifact, version)
        documentation = ElementTree.fromstring(nar.read("META-INF/docs/extension-manifest.xml"))
        for key, value in (("groupId", GROUP), ("artifactId", "nifi-opcua-nar"), ("version", version)):
            require([entry.text for entry in documentation.findall(key)] == [value],
                    "Extension manifest bundle coordinates do not match: " + key)
        extensions = documentation.findall("extensions/extension")
        require(len(extensions) == 1
                and [entry.text for entry in extensions[0].findall("name")] == [PROCESSOR],
                "Expected exactly one ConsumeOPCUA extension")
        require([entry.text for entry in extensions[0].findall("type")] == ["PROCESSOR"],
                "Expected PROCESSOR extension type")
        require("META-INF/LICENSE" in nar.namelist(), "NAR project license is missing")
        for suffix in ("html", "md"):
            require(f"META-INF/docs/additional-details/{PROCESSOR}/additionalDetails.{suffix}" in nar.namelist(),
                    "NAR processor details are missing: " + suffix)
        jars = [name for name in nar.namelist() if name.endswith(".jar")]
        require(not any(Path(name).name.startswith("nifi-opcua-subscriber-processors-") for name in jars),
                "Retired processor JAR is present")
        forbidden = ("/nifi-api-", "/sdk-server-", "/milo-sdk-server-", "/slf4j-api-",
                     "/slf4j-simple-", "/junit-", "/mockito-")
        require(not any(part in name for name in jars for part in forbidden),
                "Runtime contains provided/test-only dependencies")
        for name in jars:
            with zipfile.ZipFile(io.BytesIO(nar.read(name))) as jar:
                reject_retired_content(jar)
                if SERVICE in jar.namelist():
                    providers.extend(service_providers(jar))
                filename = Path(name).name
                is_processor = any(filename.startswith(artifact + "-") or
                                   f"META-INF/maven/{GROUP}/{artifact}/pom.properties" in jar.namelist()
                                   for artifact in processor_ids)
                if is_processor:
                    require(filename == f"{processor_artifact}-{version}.jar",
                            "Unexpected processor JAR for this target: " + name)
                    processors += 1
                    verify_maven_coordinates(jar, processor_artifact, version)
                    require(service_providers(jar) == [PROCESSOR],
                            "Expected exactly one ConsumeOPCUA service registration")
                    require("META-INF/LICENSE" in jar.namelist(), "Processor JAR license is missing")
                    require(jar.namelist().count(PROCESSOR.replace(".", "/") + ".class") == 1,
                            "Expected exactly one ConsumeOPCUA class entry")
                    for suffix in ("html", "md"):
                        require(f"docs/{PROCESSOR}/additionalDetails.{suffix}" in jar.namelist(),
                                "Processor JAR details are missing: " + suffix)
                for entry in jar.namelist():
                    if re.fullmatch(processor_class_pattern, entry):
                        processor_classes += 1
                    if not entry.endswith(".class") or entry.startswith("META-INF/versions/") or entry.endswith("module-info.class"):
                        continue
                    data = jar.read(entry)
                    require(len(data) >= 8 and data[:4] == b"\xca\xfe\xba\xbe", "Invalid class file: " + entry)
                    major = struct.unpack(">H", data[6:8])[0]
                    require(major <= maximum, f"Incompatible bytecode: {name}: {entry}: {major} > {maximum}")
                    if is_processor:
                        require(major == maximum, f"Incompatible bytecode: {name}: {entry}: expected {maximum}, found {major}")
                    classes += 1
        require(processors == 1, "Expected exactly one processor JAR")
        require(processor_classes == 1, "Expected exactly one ConsumeOPCUA class entry across all bundled JARs")
        require(providers == [PROCESSOR], "Expected exactly one ConsumeOPCUA service registration across all bundled JARs")
        require(classes > 0, "NAR contains no base classes")
    return len(jars), classes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("nar", type=Path)
    parser.add_argument("maximum", type=int, choices=[52, 65])
    args = parser.parse_args()
    jars, classes = verify(args.nar, args.maximum)
    print(f"{args.nar.name}: validated {jars} bundled JARs and {classes} classes (maximum class version {args.maximum})")
