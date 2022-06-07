/*
 * Copyright [2016-2020] [George Papadakis (gpapadis@yahoo.gr)]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scify.jedai.version3.prioritization;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.opencsv.CSVWriter;
import org.apache.log4j.BasicConfigurator;
import org.checkerframework.checker.units.qual.A;
import org.scify.jedai.blockbuilding.IBlockBuilding;
import org.scify.jedai.blockbuilding.StandardBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.blockcleaning.ComparisonsBasedBlockPurging;
import org.scify.jedai.blockprocessing.comparisoncleaning.CardinalityEdgePruning;
import org.scify.jedai.blockprocessing.comparisoncleaning.CardinalityNodePruning;
import org.scify.jedai.datamodel.*;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.entityclustering.ConnectedComponentsClustering;
import org.scify.jedai.entityclustering.IEntityClustering;
import org.scify.jedai.entitymatching.IEntityMatching;
import org.scify.jedai.entitymatching.ProfileMatcher;
import org.scify.jedai.prioritization.IPrioritization;
import org.scify.jedai.prioritization.ProgressiveEntityScheduling;
import org.scify.jedai.prioritization.ProgressiveGlobalTopComparisons;
import org.scify.jedai.utilities.ClustersPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.RepresentationModel;
import org.scify.jedai.utilities.enumerations.SimilarityMetric;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

public class TestProgressiveDirtyER {
    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();

        String no = "500k";
        String queries = "queries1";


        String mainDir = "data/queryERDatasets/";
        String[] profilesFile = {"papers" + no};
        String[] groundtruthFile = {profilesFile[0] + "Duplicates"};
        String queryERPath = "../queryER/queryER-experiments/oag/links/" + no + "/" + queries;
        File outDir = new File("../queryER/queryER-experiments/oag/jedai/" + profilesFile[0] + "/");

        if(!(outDir.exists()))  outDir.mkdir();
        File queriesFile = new File(outDir.getAbsolutePath() + queries);

        FileWriter outputfile = new FileWriter(queriesFile);
        CSVWriter writer = new CSVWriter(outputfile);
        File folder = new File(queryERPath);
        File[] listOfFiles = folder.listFiles();
        List<String> headerList = new ArrayList<>();
        headerList.add("Current Time");
        headerList.add("Total Recall");
        assert listOfFiles != null;
        headerList = Arrays.stream(listOfFiles).map(File::getName).collect(Collectors.toList());
        String[] header = new String[headerList.size()];

        headerList.toArray(header);
        writer.writeNext(header);

        for (int i = 0; i < groundtruthFile.length; i++) {
            IEntityReader eReader = new EntitySerializationReader(mainDir + profilesFile[i]);
            List<EntityProfile> profiles = eReader.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles.size());
            double start = System.currentTimeMillis();
            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthFile[i]);
            final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            IBlockBuilding blockBuildingMethod = new StandardBlocking();
            List<AbstractBlock> blocks = blockBuildingMethod.getBlocks(profiles);
            System.out.println("Original blocks\t:\t" + blocks.size());

            double bpStart = System.currentTimeMillis();
            IBlockProcessing blockCleaningMethod1 = new ComparisonsBasedBlockPurging(false);
            blocks = blockCleaningMethod1.refineBlocks(blocks);
            double bpEnd = System.currentTimeMillis();

            double bfStart = System.currentTimeMillis();
            IBlockProcessing blockCleaningMethod2 = new BlockFiltering(0.5f);
            blocks = blockCleaningMethod2.refineBlocks(blocks);
            double bfEnd = System.currentTimeMillis();

            double npStart = System.currentTimeMillis();
            IBlockProcessing comparisonCleaningMethod = new CardinalityNodePruning(WeightingScheme.JS);
            List<AbstractBlock> cnpBlocks = comparisonCleaningMethod.refineBlocks(blocks);
            double npEnd = System.currentTimeMillis();


            float totalComparisons = 0;

            for (AbstractBlock block : cnpBlocks) {
                totalComparisons += block.getNoOfComparisons();
            }

            final IPrioritization prioritization = new ProgressiveGlobalTopComparisons((int) totalComparisons, WeightingScheme.JS);
 //           PPS method
            prioritization.developBlockBasedSchedule(cnpBlocks);
            final IEntityMatching em = new ProfileMatcher(profiles, RepresentationModel.TOKEN_UNIGRAMS, SimilarityMetric.JACCARD_SIMILARITY);//bestModels[i], bestMetrics[i]);
            int counter = 0;
            double resStart = System.currentTimeMillis();
            boolean flag = false;
            while (prioritization.hasNext()) {
                Comparison c1 = prioritization.next();
                float similarity = em.executeComparison(c1);
                c1.setUtilityMeasure(similarity);

                duplicatePropagation.isSuperfluous(c1.getEntityId1(), c1.getEntityId2());
                counter++;
                Double recall = (double)duplicatePropagation.getNoOfDuplicates()/duplicatePropagation.getExistingDuplicates();
//                if(counter % 1000000 == 0) {
//                    System.out.println("Total Recall\t:\t" + recall);
//                    double[] queriesRecall = duplicatePropagation.queryDuplicates(queryERPath);
//                }
                Double currentTime = (System.currentTimeMillis() - resStart) /1000;
                if(currentTime > 6.0 && !flag) {
                    System.out.println(currentTime);
                    System.out.println("Total Recall\t:\t" + recall);
                    ArrayList<String> lineList = new ArrayList<>();
                    lineList.add(currentTime.toString());
                    lineList.add(recall.toString());
                    ArrayList<String> queriesRecall = duplicatePropagation.queryDuplicates(queryERPath);
                    lineList.addAll(queriesRecall);
                    String[] line = new String[lineList.size()];
                    lineList.toArray(line);
                    flag = true;
                    writer.writeNext(line);
                }
                if(recall == 1.0) break;
            }
            double resEnd = System.currentTimeMillis();

            double end = System.currentTimeMillis();
            System.out.println("BP Time\t:\t" + ((bpEnd - bpStart)/1000));
            System.out.println("BF Time\t:\t" + ((bfEnd - bfStart)/1000));
            System.out.println("NP Time\t:\t" + ((npEnd - npStart)/1000));
            System.out.println("Resolution Time\t:\t" + ((resEnd - resStart)/1000));
            System.out.println("Time\t:\t" + ((end - start)/1000));
            System.out.println("Recall\t:\t" + (double)duplicatePropagation.getNoOfDuplicates()/duplicatePropagation.getExistingDuplicates());
//            int tcomps = 0;
//            for(AbstractBlock b: cnpBlocks){
//                tcomps += b.getNoOfComparisons();
//            }
//            System.err.println(tcomps);
            System.err.println("Comps: "+counter);
            writer.close();
        }
    }
}

