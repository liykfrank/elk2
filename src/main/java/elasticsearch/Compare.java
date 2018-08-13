package elasticsearch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Compare {
	
	public static void main(String[] args) throws IOException {
		
		List<String> lines = Files.readAllLines(Paths.get("c://Users/wangjm@iata.org/Desktop/snow.txt"));
		List<String> linesNPE = Files.readAllLines(Paths.get("c://Users/wangjm@iata.org/Desktop/npe.txt"));
	
		for (int i = 0; i < lines.size(); i++) {
			for (int j = 0; j < linesNPE.size(); j++) {
				if (lines.get(i).equals(linesNPE.get(j))) {
					lines.remove(i);
				}
			}
		}
		System.out.println(lines.size());
		for (String notNPE : lines) {
			System.out.println(notNPE);
		}
	}

}
