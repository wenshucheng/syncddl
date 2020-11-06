package com.huacloud.synctable.mapping;

import com.huacloud.synctable.dialect.Dialect;

import java.util.Iterator;

public class PrimaryKey extends Constraint {

	public String sqlConstraintString(Dialect dialect) {
		if (getColumnSize() == 0) {
			return "";
		}
		StringBuilder buf = new StringBuilder("PRIMARY KEY (");
		Iterator iter = columnIterator();
		while ( iter.hasNext() ) {
			Object next = iter.next();
			if (next != null) {
				buf.append( ( (Column) next).getQuotedName(dialect) );
				if ( iter.hasNext() ) {
					buf.append(", ");
				}
			}
		}
		return buf.append(')').toString();
	}

	@Override
	public String sqlConstraintString(
			Dialect dialect, String constraintName,
			String defaultCatalog, String defaultSchema) {
		if (getColumnSize() == 0) {
			return "";
		}
		StringBuilder buf = new StringBuilder(dialect.getAddPrimaryKeyConstraintString(constraintName)).append('(');
		Iterator iter = columnIterator();
		while (iter.hasNext()) {
			buf.append(((Column) iter.next()).getQuotedName(dialect));
			if (iter.hasNext()) {
				buf.append(", ");
			}
		}
		return buf.append(')').toString();
	}

	@Override
	public String generatedConstraintNamePrefix() {
		return "PK_";
	}
}
