package s2s.experiments;

import s2s.bench_generator.Generator;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class ConvertNewProject {


	public static Generator generateTPCH(String benchFolderName, String packageFolder)
			throws SQLException, IOException {

        File duckdbFile = new File("TPCH-duckdb/tpch.db");
		String duckdbPath = duckdbFile.getAbsolutePath();
		String jdbcUrl = "jdbc:duckdb:" + duckdbPath;
		String basedir = System.getProperty("basedir");
		File benchFolder = new File(basedir, benchFolderName);

		File outputDir = new File(benchFolder, "src/main/java/" + packageFolder);
		outputDir.mkdirs();

		File testOutputDir = new File(benchFolder, "src/test/java/" + packageFolder);
		testOutputDir.mkdirs();

		Generator generator = new Generator(packageFolder, outputDir.getAbsolutePath(), testOutputDir.getAbsolutePath());

		generator.generateSchema(jdbcUrl);
        return generator;
	}
}
