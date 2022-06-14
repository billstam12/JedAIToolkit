package org.scify.jedai.version3.prioritization;


import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class CreateData {

    private static final String COMMA_DELIMITER = ",";
    private static final String TAB_DELIMETER = "\t";

    public static CsvParser openCsv(String path) throws IOException {
        // The settings object provides many configuration options
        CsvParserSettings parserSettings = new CsvParserSettings();
        //You can configure the parser to automatically detect what line separator sequence is in the input
        parserSettings.setNullValue("");
        parserSettings.setEmptyValue("");
        parserSettings.setDelimiterDetectionEnabled(true);
        CsvParser parser = new CsvParser(parserSettings);
//		parser.beginParsing(new File(source.path()));
        parser.beginParsing(new File(path), Charset.forName("US-ASCII"));
        return parser;
    }


    public static void main(String[] args) throws IOException {
        String path = "";
        String file = "projects";

            path = "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/oag/" + file + ".csv";
            path = "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/synthetic/" + file + ".csv";

            List<EntityProfile> entityProfiles = new ArrayList<>();
            CsvParser parser = openCsv(path);
            String[] header = parser.parseNext();
            String[] nextLine = parser.parseNext();
            while (nextLine != null){
                int key = 0;
                int index = 0;
                EntityProfile eP = new EntityProfile(Integer.toString(Integer.parseInt(nextLine[key])));
                while (index < nextLine.length) {
                    if (index != key) {
                        eP.addAttribute(header[index], nextLine[index]);
                    }
                    index++;
                }
                entityProfiles.add(eP);
                nextLine = parser.parseNext();
            }
            EntitySerializationReader entitySerializationReader = new EntitySerializationReader( "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/progressive-data/" + file);

            entitySerializationReader.storeSerializedObject(entityProfiles,
                    "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/progressive-data/" + file);

            path = "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/ground_truth_synthetic/ground_truth_" + file + ".csv";
            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
                String line;
                Set<IdDuplicates> groundDups = new HashSet<>();
                br.readLine();
                while ((line = br.readLine()) != null) {
                    String[] values = line.split(COMMA_DELIMITER);
                    Integer id_d = Integer.parseInt(values[0]);
                    Integer id_s = Integer.parseInt(values[1]);

                    IdDuplicates idd = new IdDuplicates(id_d, id_s);
                    groundDups.add(idd);
                }
                entitySerializationReader = new EntitySerializationReader( "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/progressive-data/groundTruth" + file + "Duplicates");

                entitySerializationReader.storeSerializedObject(groundDups,
                        "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/progressive-data/groundTruth" + file + "Duplicates");
            }
    }
}
