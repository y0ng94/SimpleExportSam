## Getting Started

Database 레코드를 별도의 툴의 도움 없이 SAM 파일의 형식으로 추출하는 프로그램

## Folder Structure

- `src`     : 소스 코드를 관리하는 디렉토리
- `bin`  	: 소스를 컴파일한 Class 파일을 관리하는 디렉토리
- `lib`  	: 프로그램 내에서 사용하는 라이브러리를 관리하는 디렉토리
- `conf`	: 환경설정 파일과 SQL문에 바인딩할 파라미터들이 작성된 리스트 파일의 기본 경로
- `file`	: 추출된 SAM 파일을 저장하는 기본 경로
- `log`		: 로그 파일 저장 기본 경로
- `shell`	: Windows 혹은 Unix, Linux에서 실행 및 컴파일 하기 위한 쉘을 관리하는 디렉토린

## Dependency Management

- 'com.batch.module.SimpleExportSam'	: Jdk 1.8, Jdbc driver

- 'yn.util.CommonUtil'					: Jdk 1.8
- 'yn.util.Config'						: Jdk 1.8
- 'yn.util.LogUtil'						: slf4j, Log library
- 'yn.util.TablePrinterUtil'			: Jdk 1.8

## Code Description

- 'com.batch.module.SimpleExportSam'	: SAM 파일 추출 프로그램

- 'yn.util.CommonUtil'					: 공통적으로 사용 가능한 유틸리티
- 'yn.util.Config'						: Pure Java 모듈에서 Property를 로드 및 관리하기 위한 유틸리티
- 'yn.util.LogUtil'						: 로그 출력에 있어서 MessageFormat을 이용한 파라미터 입력을 지원하는 유틸리티
- 'yn.util.TablePrinterUtil'			: Pure Java 모듈에서 테이블 형태의 데이터를 출력하기 위한 유틸리티