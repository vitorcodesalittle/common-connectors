package br.com.vilmasoftware;

import br.com.vilmasoftware.cli.CliConfiguration;

public class Main {
	public static void main(String[] args) {
		var cliConfig = new CliConfiguration(args);
		cliConfig.run();
	}

}
