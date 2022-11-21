import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.batch.module.SimpleExportSam;

import yn.util.CommonUtil;
import yn.util.Config;
import yn.util.LogUtil;

public class App {
	private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
		// Logging start
		LogUtil.info(logger, "Start export sam.");
		// Start time
		long time = System.currentTimeMillis();
		// Base directory of project
		String baseDir = "./";

		// First argument check (Base directory)
		if (args.length >= 1) { baseDir = args[0]; }
		// Add separator in base directory
		if (!baseDir.endsWith("/") && !baseDir.endsWith("\\")) { baseDir = baseDir + File.separator; }

		// Setting configuration
		try {
			Config.setConfig(baseDir + "conf/conf.properties");

			// Logging configuration table
			LogUtil.info(logger, "\n" + Config.getConfigTable());

			Config.setConfig("BASEDIR", baseDir);
			Config.setConfig("TIME", new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(time)));
		} catch (Exception e) {
			e.printStackTrace();
			LogUtil.error(logger, "Error during setting configuration, Check your configuration properties -> " + e.getMessage());
			return;
		}

		// process start
		new SimpleExportSam().run();

		// Logging end
		LogUtil.info(logger, "End export sam. ( {0}s )", CommonUtil.getTimeElapsed(time));
    }
}
