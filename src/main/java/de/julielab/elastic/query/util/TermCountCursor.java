package de.julielab.elastic.query.util;

import de.julielab.elastic.query.components.data.IFacetField;
import de.julielab.elastic.query.components.data.IFacetField.FacetType;

public interface TermCountCursor {
	public boolean forwardCursor();
	public String getName();
	public Number getFacetCount(FacetType type);
	public long size();
	public boolean isValid();
	public void reset();
}
