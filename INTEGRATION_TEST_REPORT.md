# csvconv 통합 테스트 리포트

**작성일**: 2026-02-10

---

## 환경 정보

| 항목 | 값 |
|------|-----|
| 플랫폼 | macOS Darwin 24.2.0 |
| Python | 3.9.6 |
| PyArrow | 16.1.0 |
| pytest | 7.4.3 |

---

## 테스트 요약

| 구분 | 결과 |
|------|------|
| 전체 테스트 | 112개 |
| 단위 테스트 | 91개 통과 |
| 통합 테스트 | 19개 통과 |
| E2E 테스트 | 2개 통과 |
| 총 실행 시간 | 1.74초 |
| **최종 결과** | **✅ 모든 테스트 통과** |

---

## 자동화된 테스트 결과 (pytest)

### 단위 테스트 (91개 통과, 0.39초)

```
tests/unit/test_cli.py (20 tests) - CLI 인자 파싱, 검증, 자동 감지
tests/unit/test_converter.py (3 tests) - CSV to Parquet 변환기
tests/unit/test_csv_reader.py (9 tests) - 스트리밍 CSV 리더
tests/unit/test_csv_writer.py (12 tests) - NFS-안전 원자성 쓰기 기능이 있는 CSV 라이터
tests/unit/test_edge_cases.py (7 tests) - 유니코드, 넓은 CSV, 단일 행, 덮어쓰기
tests/unit/test_memory_bounded.py (3 tests) - 메모리 제한 스트리밍 검증
tests/unit/test_parquet_writer.py (5 tests) - 증분 Parquet 라이터
tests/unit/test_schema_inference.py (4 tests) - tar.gz에서 스키마 추론
tests/unit/test_schema_validation.py (4 tests) - 스키마 검증
tests/unit/test_security.py (10 tests) - 경로 순회 공격 방지
tests/unit/test_smoke.py (3 tests) - 임포트 및 버전 확인
tests/unit/test_summary.py (4 tests) - 변환 요약 추적
tests/unit/test_tar_reader.py (7 tests) - tar.gz 아카이브 리더
```

### 통합 테스트 (19개 통과, 0.81초)

```
tests/integration/test_cli_integration.py (7 tests) - CLI 서브프로세스 e2e
tests/integration/test_csv_to_parquet.py (5 tests) - CSV→Parquet 파이프라인
tests/integration/test_targz_to_csv.py (4 tests) - tar.gz→CSV 추출
tests/integration/test_targz_to_parquet.py (3 tests) - tar.gz→Parquet 변환
```

### E2E 테스트 (2개 통과, 0.70초)

```
tests/e2e/test_pex.py (2 tests) - PEX 바이너리 --help 및 --version
```

---

## 실제 CLI 출력 검증

### 테스트 1: 버전 확인

```
$ python3 -m csvconv --version
csvconv 1.0.0
```

### 테스트 2: 도움말

```
$ python3 -m csvconv --help
usage: csvconv [-h] [--version] --input INPUT --output OUTPUT
               [--input-type {csv,tar.gz}] [--output-type {parquet,csv}]
               [--block-size-mb BLOCK_SIZE_MB]
               [--row-group-size ROW_GROUP_SIZE]
               [--schema-sample-rows SCHEMA_SAMPLE_ROWS] [--gzip]
               [--log-level {DEBUG,INFO,WARN,WARNING,ERROR}]

Memory-efficient CSV and tar.gz to Parquet/CSV converter.

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --input INPUT         Input file path (CSV or tar.gz)
  --output OUTPUT       Output file or directory path
  --input-type {csv,tar.gz}
                        Input type: "csv" or "tar.gz" (auto-detected from
                        extension if not given)
  --output-type {parquet,csv}
                        Output type: "parquet" or "csv" (default: parquet)
  --block-size-mb BLOCK_SIZE_MB
                        Block size in MB for CSV reading (default: 1, must be
                        > 0)
  --row-group-size ROW_GROUP_SIZE
                        Parquet row group size (default: None, must be > 0 if
                        given)
  --schema-sample-rows SCHEMA_SAMPLE_ROWS
                        Number of rows to sample for schema inference
                        (default: 1000, must be > 0)
  --gzip                Enable gzip compression for CSV output
  --log-level {DEBUG,INFO,WARN,WARNING,ERROR}
                        Logging level (default: INFO)
```

### 테스트 3: CSV to Parquet (사용 사례 1)

```
$ python3 -m csvconv --input sample.csv --output out.parquet
INFO: Converted: sample.csv -> out.parquet
Conversion Summary
========================================
Success: 1 file(s)
Failure: 0 file(s)

Successful files:
  - sample.csv

--- Output File ---
-rw-------  1 nineking  wheel  2809 Feb 10 06:34 out.parquet

--- Parquet Metadata ---
num_rows: 100
num_columns: 3
num_row_groups: 1
schema:
  id: int64
  value: double
  name: string

first 5 rows:
{'id': 0, 'value': 0.0, 'name': 'name_0'}
{'id': 1, 'value': 1.5, 'name': 'name_1'}
{'id': 2, 'value': 3.0, 'name': 'name_2'}
{'id': 3, 'value': 4.5, 'name': 'name_3'}
{'id': 4, 'value': 6.0, 'name': 'name_4'}
```

### 테스트 4: tar.gz to Parquet (사용 사례 2)

```
$ python3 -m csvconv --input sample.tar.gz --output pq_out/ --output-type parquet
INFO: Converted: data_0.csv -> pq_out/data_0.parquet
INFO: Converted: data_1.csv -> pq_out/data_1.parquet
INFO: Converted: data_2.csv -> pq_out/data_2.parquet
Conversion Summary
========================================
Success: 3 file(s)
Failure: 0 file(s)

Successful files:
  - data_0.csv
  - data_1.csv
  - data_2.csv

--- Output Files ---
-rw-------  1 nineking  wheel  2017 data_0.parquet
-rw-------  1 nineking  wheel  2013 data_1.parquet
-rw-------  1 nineking  wheel  2006 data_2.parquet

--- data_0.parquet Content ---
num_rows: 50
schema: id: int64, value: double, name: string
first 3 rows:
{'id': 0, 'value': 0.0, 'name': 'name_0'}
{'id': 1, 'value': 1.5, 'name': 'name_1'}
{'id': 2, 'value': 3.0, 'name': 'name_2'}
```

### 테스트 5: tar.gz to CSV (사용 사례 3)

```
$ python3 -m csvconv --input sample.tar.gz --output csv_out/ --output-type csv
INFO: Extracted: data_0.csv -> csv_out/data_0.csv
INFO: Extracted: data_1.csv -> csv_out/data_1.csv
INFO: Extracted: data_2.csv -> csv_out/data_2.csv
Conversion Summary
========================================
Success: 3 file(s)
Failure: 0 file(s)

Successful files:
  - data_0.csv
  - data_1.csv
  - data_2.csv

--- Output Files ---
-rw-------  1 nineking  wheel  787 data_0.csv
-rw-------  1 nineking  wheel  837 data_1.csv
-rw-------  1 nineking  wheel  904 data_2.csv

--- data_0.csv (first 6 lines) ---
id,value,name
0,0.0,name_0
1,1.5,name_1
2,3.0,name_2
3,4.5,name_3
4,6.0,name_4
```

### 테스트 6: tar.gz to CSV (gzip 압축 적용)

```
$ python3 -m csvconv --input sample.tar.gz --output csv_gz_out/ --output-type csv --gzip
INFO: Extracted: data_0.csv -> csv_gz_out/data_0.csv.gz
INFO: Extracted: data_1.csv -> csv_gz_out/data_1.csv.gz
INFO: Extracted: data_2.csv -> csv_gz_out/data_2.csv.gz
Conversion Summary
========================================
Success: 3 file(s)
Failure: 0 file(s)

Successful files:
  - data_0.csv
  - data_1.csv
  - data_2.csv

--- Output Files ---
-rw-------  1 nineking  wheel  322 data_0.csv.gz
-rw-------  1 nineking  wheel  351 data_1.csv.gz
-rw-------  1 nineking  wheel  360 data_2.csv.gz

--- data_0.csv.gz decompressed (first 6 lines) ---
id,value,name
0,0.0,name_0
1,1.5,name_1
2,3.0,name_2
3,4.5,name_3
4,6.0,name_4
```

### 테스트 7: 스키마 불일치 오류 격리

```
$ python3 -m csvconv --input mismatch.tar.gz --output mismatch_out/ --output-type parquet
INFO: Converted: file1.csv -> mismatch_out/file1.parquet
ERROR: Failed to convert member file2.csv: Schema column name mismatch at position 1: expected 'value', got 'category'
INFO: Converted: file3.csv -> mismatch_out/file3.parquet
Conversion Summary
========================================
Success: 2 file(s)
Failure: 1 file(s)

Successful files:
  - file1.csv
  - file3.csv

Failed files:
  - file2.csv: Schema column name mismatch at position 1: expected 'value', got 'category'

--- Output Files ---
-rw-------  1 nineking  wheel  1236 file1.parquet
-rw-------  1 nineking  wheel  1236 file3.parquet
```

### 테스트 8: 오류 처리 (존재하지 않는 파일)

```
$ python3 -m csvconv --input /tmp/nonexistent.csv --output /tmp/out.parquet
ERROR: Failed to convert /tmp/nonexistent.csv: [Errno 2] Failed to open local file '/tmp/nonexistent.csv'. Detail: [errno 2] No such file or directory
ERROR: Conversion failed: [Errno 2] Failed to open local file '/tmp/nonexistent.csv'. Detail: [errno 2] No such file or directory
exit code: 1
```

---

## PEX 바이너리 검증

### 테스트 9: PEX 바이너리

```
$ python3 dist/csvconv.pex --version
csvconv 1.0.0

PEX file size: 31M
```

---

## 결론

**✅ 모든 112개 테스트 통과 완료**

3가지 주요 사용 사례가 정상 동작함을 확인했습니다:
- CSV → Parquet 변환
- tar.gz → Parquet 변환
- tar.gz → CSV 추출

### 검증된 핵심 기능

| 기능 | 상태 |
|------|------|
| 스트리밍 처리 (메모리 효율성) | ✅ 검증 완료 |
| NFS-안전 원자성 쓰기 (temp + fsync + os.replace) | ✅ 검증 완료 |
| 파일별 오류 격리 (스키마 불일치 발생 시 다른 파일은 계속 처리) | ✅ 검증 완료 |
| 경로 순회 공격 방지 (보안) | ✅ 검증 완료 |
| CSV 출력 gzip 압축 | ✅ 검증 완료 |
| PEX 바이너리 패키징 (31MB) | ✅ 검증 완료 |
| 유니코드 지원 (한국어, 중국어, 일본어) | ✅ 검증 완료 |

**프로덕션 배포 준비 완료**
