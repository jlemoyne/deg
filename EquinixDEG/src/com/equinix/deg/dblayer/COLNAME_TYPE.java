package com.equinix.deg.dblayer;

/**
 * @author radiumone
 *
 */
public class COLNAME_TYPE {
	public String colname = null;
	public String coltype = null;
	public int colprecis = 0;
	public int colscale = 0;
	public int key = 0;		// key = 1 for primary key; 1 for imported, 2 for exported
	public COLNAME_TYPE(String colname, String coltype,
			int colprecis, int colscale) {
		this.colname = colname;
		this.coltype = coltype;
		this.colprecis = colprecis;
		this.colscale = colscale;
	}
}

