package org.scify.jedai.version3.prioritization;

import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CreateData {

    private static final String COMMA_DELIMITER = ",";
    private static final String TAB_DELIMETER = "\t";

    public static void main(String[] args) throws IOException {
        String path = "";
        String file = "papers5m";
        Integer type = 1;
        if(type == 0) {
            path = "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/oag/" + file + ".csv";

            List<List<String>> records = new ArrayList<>();
            List<EntityProfile> entityProfiles = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
                String line;
                line = br.readLine();
                String[] header = line.split(TAB_DELIMETER);
                int id = 0;
                while ((line = br.readLine()) != null) {
                    String[] values = line.split(TAB_DELIMETER);
                    records.add(Arrays.asList(values));
                    int key = 0;
                    int index = 0;
                    EntityProfile eP = new EntityProfile(Integer.toString(id));
                    id++;
                    while (index < values.length) {
                        if (index != key) {
                            eP.addAttribute(header[index], values[index]);
                        }
                        index++;
                    }
                    entityProfiles.add(eP);
                }
            }
            EntitySerializationReader entitySerializationReader = new EntitySerializationReader( "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/progressive-data/" + file);

            entitySerializationReader.storeSerializedObject(entityProfiles,
                    "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/progressive-data/" + file);
        }
        else{
            path = "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/ground_truth_oag/ground_truth_" + file + ".csv";
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
                EntitySerializationReader entitySerializationReader = new EntitySerializationReader( "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/progressive-data/groundTruth" + file + "Duplicates");

                entitySerializationReader.storeSerializedObject(groundDups,
                        "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/progressive-data/groundTruth" + file + "Duplicates");
            }
        }
    }
}
