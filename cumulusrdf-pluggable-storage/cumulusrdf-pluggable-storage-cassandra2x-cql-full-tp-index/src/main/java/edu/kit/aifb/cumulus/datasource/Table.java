package edu.kit.aifb.cumulus.datasource;

/**
 * Table names using within Cassandra 2x pluggable storage module.
 * 
 * @author Andrea Gazzarini
 * @author Sebastian Schmidt
 * @since 1.1.0
 */
public interface Table {
	String TABLE_S_POC = "S_POC";
	String TABLE_O_SPC = "O_SPC";
	String TABLE_PO_SC = "PO_SC";
	String TABLE_PO_SC_INDEX_P = "PO_SC_INDEX_P";
	String TABLE_RN_SP_O = "SP_O_RN_NUM";
	String TABLE_RN_P_OS = "P_OS_RN_NUM";
	String TABLE_RDT_P_OS = "P_OS_RN_DT";
	String TABLE_RDT_SP_O = "SP_O_RN_DT";
	String TABLE_OC_PS = "OC_PS";
	String TABLE_OC_PS_INDEX_C = "OC_PS_INDEX_C";
	String TABLE_SC_OP = "SC_OP";
	
	/*
	 * We need this third table because we cannot get O(log(n)) guarantees when using secondary indexes and column names.
	 * For example, we could handle an (s_1, p_1, ?, c_1) query with the OC_PS index. The rows would be found using the secondary index,
	 * the subject and predicate would be used for binary search on column names of each row to check if the row contains the given query pattern.
	 * 
	 * Assuming we insert (s_i, p_i, o_i, c) for i element I subset 'natural numbers', querying for (s_j, p_j, ?, c), j element I,
	 * would result in |I| hits for the secondary index lookup. Thus, |I| rows would need to be checked for (s_j, p_j).
	 * This results in a limitless runtime for the query. So no runtime guarantees could be given.
	 */
	String TABLE_SPC_O = "SPC_O";
	String TABLE_SPC_O_INDEX_PC = "SPC_O_INDEX_PC";	
}