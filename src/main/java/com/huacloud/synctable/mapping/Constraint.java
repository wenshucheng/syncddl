package com.huacloud.synctable.mapping;

import com.huacloud.synctable.dialect.Dialect;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * @author Peng Xiaodong
 * @date 2019-4-28 10:17:33
 */
public abstract class Constraint implements Serializable {

	private String name;

	private final ArrayList<Column> columns = new ArrayList<Column>();

	private Table table;

	private final List<String> columnNames = new ArrayList<>();

	public void addAllColumnNames(List<String> columnNames) {
		for (String columnName : columnNames) {
			this.columnNames.add(columnName);
		}
	}

	public List<String> getColumnNames() {
		return this.columnNames;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
	 * If a constraint is not explicitly named, this is called to generate
	 * a unique hash using the table and column names.
	 * Static so the name can be generated prior to creating the Constraint.
	 * They're cached, keyed by name, in multiple locations.
	 *
	 * @param prefix
	 *            Appended to the beginning of the generated name
	 * @param table
	 * @param columns
	 * @return String The generated name
	 */
	public static String generateName(String prefix, Table table, Column... columns) {
		// Use a concatenation that guarantees uniqueness, even if identical names
		// exist between all table and column identifiers.

		StringBuilder sb = new StringBuilder( "table`" + table.getName() + "`" );

		// Ensure a consistent ordering of columns, regardless of the order
		// they were bound.
		// Clone the list, as sometimes a set of order-dependent Column
		// bindings are given.
		Column[] alphabeticalColumns = columns.clone();
		Arrays.sort( alphabeticalColumns, ColumnComparator.INSTANCE );
		for ( Column column : alphabeticalColumns ) {
			String columnName = column == null ? "" : column.getName();
			sb.append( "column`" + columnName + "`" );
		}
		return prefix + hashedName( sb.toString() );
	}

	/**
	 * Helper method for {@link #generateName(String, Table, Column...)}.
	 *
	 * @param prefix
	 *            Appended to the beginning of the generated name
	 * @param table
	 * @param columns
	 * @return String The generated name
	 */
	public static String generateName(String prefix, Table table, List<Column> columns) {
		return generateName( prefix, table, columns.toArray( new Column[columns.size()] ) );
	}

	/**
	 * Hash a constraint name using MD5. Convert the MD5 digest to base 35
	 * (full alphanumeric), guaranteeing
	 * that the length of the name will always be smaller than the 30
	 * character identifier restriction enforced by a few dialects.
	 *
	 * @param s
	 *            The name to be hashed.
	 * @return String The hased name.
	 */
	public static String hashedName(String s) {
		try {
			MessageDigest md = MessageDigest.getInstance( "MD5" );
			md.reset();
			md.update( s.getBytes() );
			byte[] digest = md.digest();
			BigInteger bigInt = new BigInteger( 1, digest );
			// By converting to base 35 (full alphanumeric), we guarantee
			// that the length of the name will always be smaller than the 30
			// character identifier restriction enforced by a few dialects.
			return bigInt.toString( 35 );
		}
		catch ( NoSuchAlgorithmException e ) {
			e.printStackTrace();
		}
		return "";
	}

	private static class ColumnComparator implements Comparator<Column> {
		public static ColumnComparator INSTANCE = new ColumnComparator();

		@Override
		public int compare(Column col1, Column col2) {
			return col1.getName().compareTo( col2.getName() );
		}
	}

	public void addColumn(Column column) {
		if ( !columns.contains( column ) ) {
			columns.add( column );
		}
	}


	/**
	 * @param column
	 * @return true if this constraint already contains a column with same name.
	 */
	public boolean containsColumn(Column column) {
		return columns.contains( column );
	}

	public int getColumnSize() {
		return columns.size();
	}

	public Column getColumn(int i) {
		return  columns.get( i );
	}

	public Iterator<Column> columnIterator() {
		return columns.iterator();
	}

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public boolean isGenerated(Dialect dialect) {
		return true;
	}

	public String sqlDropString(Dialect dialect, String defaultCatalog, String defaultSchema) {
		if ( isGenerated( dialect ) ) {
			return new StringBuilder()
					.append( "alter table " )
					.append( getTable().getQualifiedName( dialect, defaultCatalog, defaultSchema ) )
					.append( " drop constraint " )
					.append( dialect.quote( getName() ) )
					.toString();
		}
		else {
			return null;
		}
	}

	public String sqlCreateString(Dialect dialect, String defaultCatalog, String defaultSchema) {
		if ( isGenerated( dialect ) ) {
			String constraintString = sqlConstraintString( dialect, getName(), defaultCatalog, defaultSchema );
			if (constraintString != null) {
				StringBuilder buf = new StringBuilder( "alter table " )
						.append( getTable().getQualifiedName( dialect, defaultCatalog, defaultSchema ) )
						.append( constraintString );
				return buf.toString();
			}
		}
		return null;
	}

	public List getColumns() {
		return columns;
	}

	public abstract String sqlConstraintString(Dialect d, String constraintName, String defaultCatalog,
											   String defaultSchema);
	@Override
	public String toString() {
		return getClass().getName() + '(' + getTable().getName() + getColumns() + ") as " + name;
	}
	
	/**
	 * @return String The prefix to use in generated constraint names.  Examples:
	 * "UK_", "FK_", and "PK_".
	 */
	public abstract String generatedConstraintNamePrefix();
}
