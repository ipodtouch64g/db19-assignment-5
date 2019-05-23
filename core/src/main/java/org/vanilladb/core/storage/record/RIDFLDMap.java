package org.vanilladb.core.storage.record;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.metadata.TableInfo;

public class RIDFLDMap {
	
	private Map<recordLoc,Constant> actualLoc;
	private TableInfo ti;
	
	public RIDFLDMap(TableInfo ti)
	{
		this.ti = ti;
		this.actualLoc = new HashMap<recordLoc,Constant>();
	}
	
	public void put(recordLoc loc, Constant val)
	{
		actualLoc.put(loc, val);
	}
	public Constant get(recordLoc loc)
	{
		return actualLoc.get(loc);
	}
	public Set getEntrySet()
	{
		return actualLoc.entrySet();
	}
	
	public TableInfo getTi() 
	{
		return this.ti;
	}
}
