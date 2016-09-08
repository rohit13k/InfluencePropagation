package com.ulb.code.wit.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;

import edu.uci.ics.jung.algorithms.scoring.PageRank;

import java.util.ArrayList;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;

public class PageRankCalc {

	DirectedGraph<Integer, String> g = new DirectedSparseGraph<Integer, String>();

	private void readFile(String filename, String delim) throws IOException {

		FileReader fr = new FileReader(filename);
		BufferedReader br = new BufferedReader(fr);

		String line;

		while ((line = br.readLine()) != null) {
			String[] result = line.split(delim);
			g.addEdge(result[0] + " " + result[1], Integer.parseInt(result[0]),
					Integer.parseInt(result[1]));
		}

		br.close();
	}

	public static void main(String args[]) throws IOException {
		String[] filelist = { "slashdot-threads", "facebook-wosn-wall",
				"higgs-activity_time", "enron", "lkml-reply" };

		for (String file : filelist) {
			long starttime = new Date().getTime();
			PageRankCalc prc = new PageRankCalc();
			String folder = "C://Users//Rohit//Google Drive//testdata//";
			prc.readFile(folder + "input//" + file + ".txt", ",");
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(
					folder + "input//" + file + "_pr.csv")));
			PageRank<Integer, String> pr = new PageRank<Integer, String>(prc.g,
					0.15);
			pr.evaluate();
			double sum = 0;
			Set<Integer> sortedVerticesSet = new TreeSet<Integer>(
					prc.g.getVertices());
			for (Integer v : sortedVerticesSet) {
				double score = pr.getVertexScore(v);
				sum += score;
				bw.write(v + "," + score + "\n");
			}
			bw.close();
			System.out.println("s = " + sum + " time: "
					+ (new Date().getTime() - starttime));
		}

	}
}