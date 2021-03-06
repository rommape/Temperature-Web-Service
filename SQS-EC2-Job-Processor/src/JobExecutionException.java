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

public class JobExecutionException extends Exception {

    static final long serialVersionUID = -6626042269058435286L;

    public JobExecutionException() {
        super();
    }

    public JobExecutionException(String message) {
        super(message);
    }

    public JobExecutionException(Throwable cause) {
        super(cause);
    }

    public JobExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
