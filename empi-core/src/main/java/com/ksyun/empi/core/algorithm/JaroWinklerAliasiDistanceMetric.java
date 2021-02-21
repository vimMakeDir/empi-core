/**
 *
 * Copyright (C) 2002-2012 "SYSNET International, Inc."
 * support@sysnetint.com [http://www.sysnetint.com]
 *
 * This file is part of OpenEMPI.
 *
 * OpenEMPI is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.ksyun.empi.core.algorithm;

import java.util.Arrays;

public class JaroWinklerAliasiDistanceMetric extends AbstractDistanceMetric
{
    private final double mWeightThreshold;
    private final int mNumChars;

	public JaroWinklerAliasiDistanceMetric() {
        mWeightThreshold = 0.7;
        mNumChars = 4;
	}


    public JaroWinklerAliasiDistanceMetric(double weightThreshold, int numChars) {
        mNumChars = numChars;
        mWeightThreshold = weightThreshold;
    }

	public double score(Object value1, Object value2) {
//		if (missingValues(value1, value2)) {
//			return handleMissingValues(value1, value2);
//		}
		String string1 = value1.toString();
		String string2 = value2.toString();		
		double distance = proximity(upperCase(string1), upperCase(string2));
		log.trace("Computed the distance between :" + string1 + ": and :" + string2 + ": to be " + distance);
		return distance;
	}

    public double proximity(String cSeq1, String cSeq2) {
        int len1 = cSeq1.length();
        int len2 = cSeq2.length();
        if (len1 == 0)
            return len2 == 0 ? 1.0 : 0.0;

        int  searchRange = Math.max(0,Math.max(len1,len2)/2 - 1);

        boolean[] matched1 = new boolean[len1];
        Arrays.fill(matched1,false);
        boolean[] matched2 = new boolean[len2];
        Arrays.fill(matched2,false);

        int numCommon = 0;
        for (int i = 0; i < len1; ++i) {
            int start = Math.max(0,i-searchRange);
            int end = Math.min(i+searchRange+1,len2);
            for (int j = start; j < end; ++j) {
                if (matched2[j]) continue;
                if (cSeq1.charAt(i) != cSeq2.charAt(j))
                    continue;
                matched1[i] = true;
                matched2[j] = true;
                ++numCommon;
                break;
            }
        }
        if (numCommon == 0) return 0.0;

        int numHalfTransposed = 0;
        int j = 0;
        for (int i = 0; i < len1; ++i) {
            if (!matched1[i]) continue;
            while (!matched2[j]) ++j;
            if (cSeq1.charAt(i) != cSeq2.charAt(j))
                ++numHalfTransposed;
            ++j;
        }

        int numTransposed = numHalfTransposed/2;

        double numCommonD = numCommon;
        double weight = (numCommonD/len1
                         + numCommonD/len2
                         + (numCommon - numTransposed)/numCommonD)/3.0;

        if (weight <= mWeightThreshold) return weight;
        int max = Math.min(mNumChars,Math.min(cSeq1.length(),cSeq2.length()));
        int pos = 0;
        while (pos < max && cSeq1.charAt(pos) == cSeq2.charAt(pos))
            ++pos;
        if (pos == 0) return weight;
        return weight + 0.1 * pos * (1.0 - weight);

    }

}
