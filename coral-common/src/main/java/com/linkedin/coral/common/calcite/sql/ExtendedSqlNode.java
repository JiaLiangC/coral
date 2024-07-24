/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql;

import com.linkedin.coral.common.calcite.sql.error.SqlValidateException;


/**
 * An remark interface which should be inherited by extended sql nodes which are not supported by
 * Calcite core parser.
 *
 * <p>We need this to customize our validation rules combined with the rules defined in {@link
 * org.apache.calcite.sql.validate.SqlValidatorImpl}.
 */
public interface ExtendedSqlNode {
  void validate() throws SqlValidateException;
}
