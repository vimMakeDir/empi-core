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

import java.util.List;

public interface StringComparisonService
{
	public DistanceMetricType[] getDistanceMetricTypes();
	
	public List<String> getComparisonFunctionNames();
	
	public DistanceMetricType getDistanceMetricType(String name);
	
	public double score(String metricType, Object value1, Object value2);
	
	public double score(String metricType, java.util.Map<String, String> parameters, Object value1, Object value2);
}
