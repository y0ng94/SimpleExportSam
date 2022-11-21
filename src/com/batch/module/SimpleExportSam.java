package com.batch.module;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import yn.util.CommonUtil;
import yn.util.Config;
import yn.util.LogUtil;

public class SimpleExportSam {
	private static Logger logger = LoggerFactory.getLogger(SimpleExportSam.class);

	public void run() {
		// 기본 파라미터가 적힌 파일을 찾기, 없으면 풀 경로 검색
		long time = System.currentTimeMillis();
		LogUtil.info(logger, "Find parameter list file for query binding");

		Path paramFilePath = Paths.get(Config.getConfig("DB.PARAM"));
		if (Files.notExists(paramFilePath)) { paramFilePath = Paths.get(Config.getConfig("BASEDIR"), Config.getConfig("DB.PARAM")); }
		if (Files.notExists(paramFilePath)) {
			LogUtil.error(logger, "Parameter file does not exist...");
			return;
		}

		LogUtil.info(logger, "Completed finding parameter list file for query binding ( {0}s )", CommonUtil.getTimeElapsed(time));

		// 파라미터 파일 읽어서 리스트에 담기
		time = System.currentTimeMillis();
		LogUtil.info(logger, "Reading parameter list file for query binding");

		List<String[]> paramList = new ArrayList<>();		
		try {
			Files.readAllLines(paramFilePath).stream().forEach(t -> paramList.add(t.split("\t")));
		} catch (IOException e) {
			e.printStackTrace();
			LogUtil.error(logger,"Error occurred during reading parameter list file ({0})", e.getMessage());
			return;
		}

		LogUtil.info(logger, "Completed reading parameter list file for query binding ( {0}s )", CommonUtil.getTimeElapsed(time));

		// 메모리에 클래스 올리기
		time = System.currentTimeMillis();
		try {
			Class.forName(Config.getConfig("DB.CLASS"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LogUtil.error(logger, "Error occurred during load database driver ({0})", e.getMessage());
			return;
		}

		LogUtil.info(logger, "Completed loading database driver ( {0}s )", CommonUtil.getTimeElapsed(time));

		// 파라미터의 갯수만큼 반복
		int totalCount = 0;
		int writeCount = 0;

		String outputFile;
		try {
			outputFile = addUniquePath(Config.getConfig("OUTPUT.FILE"));
		} catch (NullPointerException | IOException e) {
			e.printStackTrace();
			LogUtil.error(logger, "Error occurred create save path ({0})", e.getMessage());
			return;
		}

		for (String[] param : paramList) {
			LogUtil.info(logger, "Checking target parameter ( {0} )", Arrays.asList(param).stream().collect(Collectors.joining(", ")));

			// 커넥션 객체 만들기
			time = System.currentTimeMillis();
			Connection con = null;
			try {
				LogUtil.info(logger, "Creating connection of database(from) ( {0} / {1} / {2} )", Config.getConfig("DB.URL"), Config.getConfig("DB.USER"), Config.getConfig("DB.PASSWORD"));
				con = getConnection(Config.getConfig("DB.URL"), Config.getConfig("DB.USER"), Config.getConfig("DB.PASSWORD"));
			} catch (NullPointerException | SQLException e) {
				e.printStackTrace();
				LogUtil.error(logger, "Error occurred during create connection of database ({0})", e.getMessage());
				return;
			}

			LogUtil.info(logger, "Completed creating connection of database ( {0}s )", CommonUtil.getTimeElapsed(time));

			// 데이터 조회해오기
			time = System.currentTimeMillis();
			List<String[]> selectList = new ArrayList<>();
			try {
				LogUtil.info(logger, "Selecting data of database");
				selectList = select(con, Config.getConfig("DB.QUERY"), param);
			} catch (NullPointerException | SQLException e) {
				e.printStackTrace();
				LogUtil.error(logger, "Error occurred during select of database ({0})", e.getMessage());
				return;
			} finally {
				if (con != null){
					try {
						con.close();
						con = null;
					} catch (SQLException e) {
						e.printStackTrace();
						LogUtil.error(logger, "Error occurred during close of database connection ({0})", e.getMessage());
						return;
					}
				}
			}

			LogUtil.info(logger, "Completed {0} selection data ( {1}s )", selectList.size(), CommonUtil.getTimeElapsed(time));

			totalCount += selectList.size();

			// 파일 쓰기
			time = System.currentTimeMillis();
			try {
				LogUtil.info(logger, "Writing data of database");
				writeCount = write(outputFile, selectList, param, writeCount);
			} catch (IOException e) {
				e.printStackTrace();
				LogUtil.error(logger, "Error occurred write file ({0})", e.getMessage());
				return;
			}

			LogUtil.info(logger, "Completed {0} write data of database ( {1}s )", selectList.size(), CommonUtil.getTimeElapsed(time));

			// 잠시 쉬기
			int sleep = Config.getIntConfig("SLEEP");
			try {
				if (paramList.indexOf(param) != paramList.size()-1) {
					LogUtil.info(logger, "Sleep for next selection. ( {0} )", sleep);
					Thread.sleep(sleep * 1000);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				LogUtil.error(logger, "Error occurred sleep ( {0}s )", e.getMessage());
				return;
			}
		}
		
		LogUtil.info(logger, "Completed all {0} processing", totalCount);
	}

	/**
	 * 커넥션 객체 만들기
	 * @param url
	 * @param user
	 * @param password
	 * @return Connection 객체
	 * @throws SQLException
	 */
	private Connection getConnection(String url, String user, String password) throws SQLException {
		Connection con = DriverManager.getConnection(url, user, password);
		con.setAutoCommit(false);
		return con;
	}

	/**
	 * 조회 하기
	 * @param con
	 * @param query
	 * @param param
	 * @return 조회 결과 리스트
	 * @throws SQLException
	 */
	private List<String[]> select(Connection con, String query, String[] param) throws SQLException {
		List<String[]> selectList = new ArrayList<>();

		try (PreparedStatement statement = con.prepareStatement(query)) {
			statement.setQueryTimeout(Config.getIntConfig("DB.TIMEOUT"));
			ResultSetMetaData metadata = statement.getMetaData();

			for (int i=0; i<param.length; i++)
				statement.setString(i+1, param[i]);

			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					String[] selectArray = new String[metadata.getColumnCount()];
					
					for (int i=1; i<=metadata.getColumnCount(); i++) {
						selectArray[i-1] = resultSet.getString(metadata.getColumnName(i));
						if (selectArray[i-1] != null && selectArray[i-1] != "")
							selectArray[i-1] = selectArray[i-1].trim();
					}

					selectList.add(selectArray);
				}
			} catch (SQLException e) {
				throw e;
			}
		} catch (SQLException e) {
			throw e;
		}

		return selectList;
	}

	/**
	 * 파일 쓰기
	 * @param outputFile
	 * @param dataList
	 * @param param
	 * @param writeCount
	 * @return 마지막 쓰고 있던 라인 번호
	 * @throws IOException
	 */
	private int write(String outputFile, List<String[]> dataList, String[] param, int writeCount) throws IOException {
		int count = writeCount;

		while (true) {
			if (dataList == null || dataList.size() == 0) { break; }

			String[] dataArray = dataList.remove(0);
			if (dataArray != null && dataArray.length != 0) {
				String writeString = Arrays.asList(dataArray).stream().collect(Collectors.joining(Config.getConfig("OUTPUT.DELIMITER")));
				Path writeFilePath = null;

				if (count >= Config.getIntConfig("OUTPUT.MAX_COUNT")) {
					writeFilePath = getNextWriteFile(Paths.get(outputFile), 0);
					count = 0;
				} else
					writeFilePath = getCurrentWriteFile(Paths.get(outputFile), 0);

				if (count == 0)
					Files.write(writeFilePath, writeString.getBytes(), StandardOpenOption.CREATE);
				else
					Files.write(writeFilePath, writeString.getBytes(), StandardOpenOption.APPEND);

				count++;

				if (count < Config.getIntConfig("OUTPUT.MAX_COUNT"))
					Files.write(writeFilePath, "\n".getBytes(), StandardOpenOption.APPEND);
			}
		}

		return count;
	}

	/**
	 * 현재 파일 검색
	 * @param filePath
	 * @param fileNumber
	 * @return 쓰고 있던 File path
	 * @throws IOException
	 */
	private Path getCurrentWriteFile(Path filePath, int fileNumber) throws IOException {
		Path returnFilePath = filePath;

		while (true) {
			String nextFile = CommonUtil.makeBackFileName(filePath.toAbsolutePath().normalize().toString(), "_" + CommonUtil.leftPad(String.valueOf(fileNumber), Config.getIntConfig("OUTPUT.FILE.NUMBER_LENGTH"), "0"));
			Path nextFilePath = Paths.get(nextFile);
			
			if (Files.notExists(nextFilePath))
				return returnFilePath;
			else {
				fileNumber += 1;
				returnFilePath = nextFilePath;
			}
		}
	}
                           
	/**
	 * 다음 파일 검색
	 * @param filePath
	 * @param fileNumber
	 * @return 써야 하는 File path
	 */
	private Path getNextWriteFile(Path filePath, int fileNumber) {
		while (true) {
			String nextFile = CommonUtil.makeBackFileName(filePath.normalize().toString(), "_" + CommonUtil.leftPad(String.valueOf(fileNumber), Config.getIntConfig("OUTPUT.FILE.NUMBER_LENGTH"), "0"));

			if (Files.notExists(Paths.get(nextFile)))
				return Paths.get(nextFile);
			else
				fileNumber += 1;
		}
	}

	/**
	 * 전달 받은 경로에 날짜 디렉토리 추가
	 * @param filePath
	 * @return 날짜 디렉토리를 추가한 filePath
	 * @throws IOException
	 */
	private String addUniquePath(String filePath) throws IOException {
		String directory = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		int index = filePath.lastIndexOf("\\");
		if (filePath.lastIndexOf("/") > index) { index = filePath.lastIndexOf("/"); 
	}
		Files.createDirectories(Paths.get(filePath.substring(0, index+1) + directory));

		return filePath.substring(0, index+1) + directory + File.separator + filePath.substring(index);
	}
}
