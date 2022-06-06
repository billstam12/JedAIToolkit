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
        Integer type = 1;
        String path = "";
        if(type == 0) {
            path = "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/oag/papers1m.csv";

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
            EntitySerializationReader entitySerializationReader = new EntitySerializationReader( "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/progressive-data/papers1m");

            entitySerializationReader.storeSerializedObject(entityProfiles,
                    "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/progressive-data/papers1m");
        }
        else{
            path = "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/queryER-data/progressive/ground_truth_papers1m.csv";
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
                EntitySerializationReader entitySerializationReader = new EntitySerializationReader( "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/progressive-data/groundTruth/papers1mDuplicates");

                entitySerializationReader.storeSerializedObject(groundDups,
                        "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/Projects/VF/progressive-data/groundTruth/papers1mDuplicates");
            }
        }
    }
}
