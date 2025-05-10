/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.row;

import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.types.RowKind;

import java.util.Objects;

/**
 * A wrapper class that associates an {@link InternalRow} with a Flink {@link RowKind} operation
 * type.
 *
 * <p>This class is used to represent a row of data along with its corresponding operation kind,
 * such as INSERT, UPDATE, DELETE, etc., as defined by Flink's {@link RowKind}.
 *
 * @param <T> the type parameter (not used directly, but allows for future extension)
 */
public class RowWithOp<T> {
    /** The internal row data. */
    private InternalRow internalRow;

    /** The kind of row operation (e.g., INSERT, UPDATE, DELETE). */
    private RowKind rowKind;

    /**
     * Constructs a {@code RowWithOp} with the specified internal row and a default operation kind.
     *
     * @param internalRow the internal row data
     */
    public RowWithOp(InternalRow internalRow) {
        this.internalRow = internalRow;
    }

    /**
     * Constructs a {@code RowWithOp} with the specified internal row and operation kind.
     *
     * @param internalRow the internal row data
     * @param operationType the kind of row operation
     */
    public RowWithOp(InternalRow internalRow, RowKind operationType) {
        this.internalRow = internalRow;
        this.rowKind = operationType;
    }

    /**
     * Returns the internal row data.
     *
     * @return the internal row
     */
    public InternalRow getInternalRow() {
        return internalRow;
    }

    /**
     * Sets the internal row data.
     *
     * @param internalRow the internal row to set
     */
    public void setInternalRow(InternalRow internalRow) {
        this.internalRow = internalRow;
    }

    /**
     * Returns the kind of row operation.
     *
     * @return the row operation kind
     */
    public RowKind getRowKind() {
        return rowKind;
    }

    /**
     * Sets the kind of row operation.
     *
     * @param rowKind the row operation kind to set
     */
    public void setRowKind(RowKind rowKind) {
        this.rowKind = rowKind;
    }

    /**
     * Indicates whether some other object is "equal to" this one. Two {@code RowWithOp} objects are
     * considered equal if their internal rows and row kinds are equal.
     *
     * @param o the reference object with which to compare
     * @return {@code true} if this object is the same as the obj argument; {@code false} otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowWithOp<?> operation = (RowWithOp<?>) o;
        return Objects.equals(internalRow, operation.internalRow) && rowKind == operation.rowKind;
    }

    /**
     * Returns a hash code value for the object.
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {
        return Objects.hash(internalRow, rowKind);
    }

    /**
     * Returns a string representation of the object.
     *
     * @return a string representation of the object
     */
    @Override
    public String toString() {
        return "Operation{" + "internalRow=" + internalRow + ", rowKind=" + rowKind + '}';
    }
}
