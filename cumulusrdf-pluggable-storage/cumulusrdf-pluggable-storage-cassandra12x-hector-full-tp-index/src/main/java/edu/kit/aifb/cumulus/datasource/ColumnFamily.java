package edu.kit.aifb.cumulus.datasource;

/**
 * Column family names used within Cassandra 1.2.x pluggable storage module.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public interface ColumnFamily {
	String S_POC = "SPOC";
	String O_SPC = "OSPC";
	String PO_SC = "POSC";
	String OC_PS = "OCPS";
	String SC_OP = "SCOP";
	String SPC_O = "SPCO";
	String RN_SP_O = "SPO_RN_NUM";
	String RN_PO_S = "POS_RN_NUM";
	String RDT_PO_S = "POS_RN_DT";
	String RDT_SP_O = "SPO_RN_DT";	
}