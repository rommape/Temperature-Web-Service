/*
 * Copyright 2008 Amazon Technologies, Inc.  Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://aws.amazon.com/apache2.0
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package de.tuhh.parallel.cw.jobprocessor;

/**
 * Interface that job execution logic must implement.
 * 
 * @author walters
 * 
 */
public interface JobExecutor {

    /**
     * Executes a job based on the provided definition.
     * 
     * @param jobDefinition
     *            string representation of job
     * @return the result
     */
    public String execute(String jobDefinition) throws JobExecutionException;

	public String execute(String jobDefinition, String messageId);
}
