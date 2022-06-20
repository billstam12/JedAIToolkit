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

import java.util.List;
import org.apache.log4j.BasicConfigurator;
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

public class TestDirtyForQueryER {
    public static void main(String[] args) {
        BasicConfigurator.configure();

        float[] bestThresholds = {0.75f, 0.45f};
//        RepresentationModel[] bestModels = {RepresentationModel.CHARACTER_BIGRAM_GRAPHS, RepresentationModel.CHARACTER_BIGRAMS_TF_IDF};
//        SimilarityMetric[] bestMetrics = {SimilarityMetric.GRAPH_OVERALL_SIMILARITY, SimilarityMetric.GENERALIZED_JACCARD_SIMILARITY};

        String mainDir = "data/queryERDatasets/";
//        String[] profilesFile = {"cddbProfiles", "coraProfiles"};
        String[] profilesFile = {"papers200k"};
//        String[] groundtruthFile = {"cddbIdDuplicates", "coraIdDuplicates"};
        String[] groundtruthFile = {"papers200kDuplicates"};

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
            IBlockProcessing blockCleaningMethod2 = new BlockFiltering(0.5F);
            blocks = blockCleaningMethod2.refineBlocks(blocks);
            double bfEnd = System.currentTimeMillis();

            double epStart = System.currentTimeMillis();
            IBlockProcessing comparisonCleaningMethod = new CardinalityEdgePruning(WeightingScheme.ARCS);
            //List<AbstractBlock> cnpBlocks = comparisonCleaningMethod.refineBlocks(blocks);
            double epEnd = System.currentTimeMillis();

            float totalComparisons = 0;
            for (AbstractBlock block : blocks) {
                totalComparisons += block.getNoOfComparisons();
            }


//            final IPrioritization prioritization = new ProgressiveGlobalTopComparisons((int) totalComparisons, WeightingScheme.JS);
//            PPS method
            final IPrioritization prioritization = new ProgressiveEntityScheduling((int) totalComparisons, WeightingScheme.ARCS);
            prioritization.developBlockBasedSchedule(blocks);


            double resStart = System.currentTimeMillis();

            final IEntityMatching em = new ProfileMatcher(profiles, RepresentationModel.TOKEN_UNIGRAMS, SimilarityMetric.JACCARD_SIMILARITY);//bestModels[i], bestMetrics[i]);
            int counter = 0;
            while (prioritization.hasNext()) {
                Comparison c1 = prioritization.next();
                float similarity = em.executeComparison(c1);
//                System.err.println(similarity);
                c1.setUtilityMeasure(similarity);

                duplicatePropagation.isSuperfluous(c1.getEntityId1(), c1.getEntityId2());
                counter++;
//
            }
            double resEnd = System.currentTimeMillis();

            double end = System.currentTimeMillis();
            System.out.println("BP Time\t:\t" + ((bpEnd - bpStart)/1000));
            System.out.println("BF Time\t:\t" + ((bfEnd - bfStart)/1000));
            System.out.println("NP Time\t:\t" + ((epEnd - epStart)/1000));
            System.out.println("Resolution Time\t:\t" + ((resEnd - resStart)/1000));
            System.out.println("Time\t:\t" + ((end - start)/1000));
            System.out.println("Recall\t:\t" + (double)duplicatePropagation.getNoOfDuplicates()/duplicatePropagation.getExistingDuplicates());
//            int tcomps = 0;
//            for(AbstractBlock b: cnpBlocks){
//                tcomps += b.getNoOfComparisons();
//            }
//            System.err.println(tcomps);
            System.err.println("Comps: "+counter);
        }
    }
}

