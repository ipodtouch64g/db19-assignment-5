package org.vanilladb.core.storage.record;

public class recordLoc {
	public final RecordId rid;
	public final String fldName;
	
	public recordLoc(RecordId rid,String fldName) {
		this.rid = rid;
		this.fldName = fldName;
	}
	
	@Override
    public boolean equals(Object that) {
        if(that instanceof recordLoc) {
        	recordLoc p = (recordLoc) that;
            return this.rid.equals(p.rid) && this.fldName.equals(p.fldName);
        }
        return false;
    }
    @Override
    public int hashCode() {
        return 41 * (41 + rid.hashCode()) + fldName.hashCode();
    }

	
}
