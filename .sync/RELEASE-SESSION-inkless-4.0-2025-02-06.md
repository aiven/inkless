# Release Sync Session: inkless-4.0 to 4.0.1

**Date**: 2025-02-06
**Release Branch**: inkless-4.0
**Working Branch**: inkless-4.0-sync-4.0.1
**Target**: 4.0.1
**Commits to merge**: 76

## Version Changes

| File | From | To |
|------|------|-----|
| gradle.properties | 4.0.0-inkless | 4.0.1-inkless |
| tests/kafkatest/__init__.py | 4.0.0.inkless | 4.0.1.inkless |
| tests/kafkatest/version.py | 4.0.0-inkless-SNAPSHOT | 4.0.1-inkless-SNAPSHOT |
| docs/js/templateData.js | 4.0.0-inkless | 4.0.1-inkless |
| committer-tools/kafka-merge-pr.py | 4.0.0-inkless | 4.0.1-inkless |
| streams/quickstart/pom.xml | 4.0.0-inkless | 4.0.1-inkless |
| streams/quickstart/java/pom.xml | 4.0.0-inkless | 4.0.1-inkless |
| streams/quickstart/java/.../pom.xml | 4.0.0-inkless | 4.0.1-inkless |

## Conflicts (11 files)

### 1. .gitignore
**Resolution**: Keep both - inkless added `_data/` directory
```
__pycache__

_data/
```

### 2. gradle.properties
**Resolution**: Use inkless version pattern
```
version=4.0.1-inkless
```

### 3. tests/kafkatest/__init__.py
**Resolution**: Use inkless version pattern
```python
__version__ = '4.0.1.inkless'
```

### 4. tests/kafkatest/version.py
**Resolution**: Use inkless version pattern
```python
DEV_VERSION = KafkaVersion("4.0.1-inkless-SNAPSHOT")
```

### 5. docs/js/templateData.js
**Resolution**: Use inkless version pattern
```javascript
"version": "40inkless",
"dotVersion": "4.0-inkless",
"fullDotVersion": "4.0.1-inkless",
```

### 6. committer-tools/kafka-merge-pr.py
**Resolution**: Use inkless version pattern
```python
DEFAULT_FIX_VERSION = os.environ.get("DEFAULT_FIX_VERSION", "4.0.1-inkless")
```

### 7. gradle/dependencies.gradle
**Resolution**: Take upstream changes (new dependencies added)
- Added: commonsBeanutils, commonsLang
- Removed: commonsIo (ZooKeeper dependency no longer needed)

### 8. streams/quickstart/pom.xml
**Resolution**: Use inkless version pattern
```xml
<version>4.0.1-inkless</version>
```

### 9. streams/quickstart/java/pom.xml
**Resolution**: Use inkless version pattern
```xml
<version>4.0.1-inkless</version>
```

### 10. streams/quickstart/java/src/main/resources/archetype-resources/pom.xml
**Resolution**: Use inkless version pattern
```xml
<kafka.version>4.0.1-inkless</kafka.version>
```

### 11. core/src/test/scala/unit/kafka/server/metadata/BrokerMetadataPublisherTest.scala
**Resolution**: Take upstream import reorganization, remove duplicate imports

## Progress

- [x] Start merge
- [x] Resolve .gitignore
- [x] Resolve gradle.properties
- [x] Resolve tests/kafkatest/__init__.py
- [x] Resolve tests/kafkatest/version.py
- [x] Resolve docs/js/templateData.js
- [x] Resolve committer-tools/kafka-merge-pr.py
- [x] Resolve gradle/dependencies.gradle (kept commonsIo + added new deps)
- [x] Resolve streams/quickstart/pom.xml (kept upstream 4.0.1)
- [x] Resolve streams/quickstart/java/pom.xml (kept upstream 4.0.1)
- [x] Resolve streams/quickstart/java/.../pom.xml (kept upstream 4.0.1)
- [x] Resolve BrokerMetadataPublisherTest.scala
- [x] Commit merge
- [x] Verify build (make build - PASSED)
- [x] Verify tests (make test - PASSED)
- [ ] Push changes

## Merge Commit

```
bbedf3248e Merge upstream 4.0.1 into inkless-4.0-sync-4.0.1
```

## API Adaptation

### LogAppendInfo constructor change

Upstream removed `shallowOffsetOfMaxTimestamp` parameter from `LogAppendInfo` constructor.

**File**: `storage/inkless/src/main/java/io/aiven/inkless/produce/UnifiedLog.java`

**Fix**: Removed the extra `RecordBatch.NO_TIMESTAMP` parameter from constructor call.

**Commit**: `bd9a88089c fix(inkless): adapt to LogAppendInfo constructor change in 4.0.1`
