/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.error;

public class ParseException extends RuntimeException {

  private static final long serialVersionUID = -6721968786653128017L;

  public ParseException() {
    super();
  }

  public ParseException(String message) {
    super(message);
  }

  public ParseException(Throwable cause) {
    super(cause);
  }

  public ParseException(String message, Throwable cause) {
    super(message, cause);
  }
}
