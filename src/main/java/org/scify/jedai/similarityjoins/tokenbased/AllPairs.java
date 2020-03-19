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

package org.scify.jedai.similarityjoins.tokenbased;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import javafx.util.Pair;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.SimilarityPairs;
import org.scify.jedai.datamodel.joins.IntPair;
import org.scify.jedai.datamodel.joins.ListItemPPJ;

/**
 *
 * @author mthanos
 */
public class AllPairs extends AbstractTokenBasedJoin {

    private int[] originalId;
    private final List<String> attributeValues;
    private TIntList[] records;

    public AllPairs(double thr) {
        super(thr);
        attributeValues = new ArrayList<>();
    }

    @Override
    protected SimilarityPairs applyJoin(String attributeName1, String attributeName2, List<EntityProfile> dataset1, List<EntityProfile> dataset2) {
        init();
        final List<Comparison> comparisons = performJoin();
        return getSimilarityPairs(comparisons);
    }

    private int getOverlap(int x, int y, int requireOverlap, int... poslen) {
        int posx = poslen.length > 0 ? poslen[0] : 0;
        int posy = poslen.length > 1 ? poslen[1] : 0;
        int currentOverlap = poslen.length > 2 ? poslen[2] : 0;

        while (posx < records[x].size() && posy < records[y].size()) {
            if (records[x].size() - posx + currentOverlap < requireOverlap
                    || records[y].size() - posy + currentOverlap < requireOverlap) {
                return -1;
            }
            if (records[x].get(posx) == records[y].get(posy)) {
                currentOverlap++;
                posx++;
                posy++;
            } else if (records[x].get(posx) < records[y].get(posy)) {
                posx++;
            } else {
                posy++;
            }
        }
        return currentOverlap;
    }

    private void init() {

        int counter = 0;
        final List<Pair<String, Integer>> idIdentifier = new ArrayList<>();
        for (EntityProfile profile : profilesD1) {
            final String nextValue = getAttributeValue(attributeNameD1, profile);
            idIdentifier.add(new Pair<>(nextValue, counter++));
        }

        if (isCleanCleanER) {
            for (EntityProfile profile : profilesD2) {
                final String nextValue = getAttributeValue(attributeNameD2, profile);
                idIdentifier.add(new Pair<>(nextValue, counter++));

            }
        }

        idIdentifier.sort((s1, s2) -> s1.getKey().split(" ").length - s2.getKey().split(" ").length);

        attributeValues.clear();
        originalId = new int[noOfEntities];
        records = new TIntList[noOfEntities];
        for (int i = 0; i < noOfEntities; i++) {
            final Pair<String, Integer> currentPair = idIdentifier.get(i);
            attributeValues.add(currentPair.getKey());
            originalId[i] = currentPair.getValue();
            records[i] = new TIntArrayList();
        }

        for (int sIndex = 0; sIndex < noOfEntities; sIndex++) {

            final String s = attributeValues.get(sIndex).trim();
            if (s.length()<1) continue;

            String[] split = s.split(" ");
            for (int sp = 0; sp < split.length; sp++) {
                int token = djbHash(split[sp]);
                records[sIndex].add(token);
            }
            records[sIndex].sort();

        }
    }

    private List<Comparison> performJoin() {
        final List<Comparison> executedComparisons = new ArrayList<>();
        final TIntObjectMap<ListItemPPJ> index = new TIntObjectHashMap<>();
        for (int k = 0; k < noOfEntities; k++) {
            final TIntList record = records[k];

            int minLength = minPossibleLength(record.size());
            int probeLength = probeLength(record.size());
            int indexLength = indexLength(record.size());

            final int[] requireOverlaps = new int[record.size() + 1];
            for (int l = minLength; l <= record.size(); l++) {
                requireOverlaps[l] = requireOverlap(record.size(), l);
            }

            final TIntIntMap occurances = new TIntIntHashMap();
            for (int t = 0; t < probeLength; t++) {
                int token = record.get(t);

                ListItemPPJ item = index.get(token);
                if (item == null) {
                    item = new ListItemPPJ();
                    index.put(token, item);
                }

                int pos = item.getPos();
                final List<IntPair> ids = item.getIds();
                int noOfIds = ids.size();
                while (pos < noOfIds && records[ids.get(pos).getKey()].size() < minLength) {
                    pos++;
                }

                for (int p = pos; p < noOfIds; p++) {
                    int candId = ids.get(p).getKey();
                    int oldValue = occurances.get(candId);
                    occurances.put(candId, (oldValue + 1));
                }

                if (t < indexLength) {
                    ids.add(new IntPair(k, t));
                }
            }

            for (int cand : occurances.keys()) {
                if (k == cand) {
                    continue;
                }

                if (isCleanCleanER) {
                    if (originalId[k] < datasetDelimiter && originalId[cand] < datasetDelimiter) { // both belong to dataset 1
                        continue;
                    }

                    if (datasetDelimiter <= originalId[k] && datasetDelimiter <= originalId[cand]) { // both belong to dataset 2
                        continue;
                    }
                }

                int noOfCandidates = records[cand].size();
                int newindexLength = indexLength(noOfCandidates);
                if (records[cand].get(newindexLength - 1) < records[k].get(probeLength - 1)) {
                    if (occurances.get(cand) + noOfCandidates - newindexLength < requireOverlaps[noOfCandidates]) {
                        continue;
                    }
                } else {
                    if (occurances.get(cand) + records[k].size() - probeLength < requireOverlaps[noOfCandidates]) {
                        continue;
                    }
                }

                int realOverlap = getOverlap(k, cand, requireOverlaps[noOfCandidates]);
                if (realOverlap != -1) {
                    double jaccardSim = calcSimilarity(records[k].size(), noOfCandidates, realOverlap);
                    if (jaccardSim >= threshold) {
                        final Comparison currentComp = getComparison(originalId[k], originalId[cand]);
                        currentComp.setUtilityMeasure(jaccardSim); // is this correct?
                        executedComparisons.add(currentComp);
                    }
                }
            }
        }
        return executedComparisons;
    }
}