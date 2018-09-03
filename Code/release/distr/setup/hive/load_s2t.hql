drop table if exists custom_cb_k7m.S2T_Stage;
create external table custom_cb_k7m.S2T_Stage (
  Scheme_LD String,
	Scheme_OD String,	
  Table String,	
  Field	String, 
  Type_LD String,	
  Type_OD String,	
  Validation_LD String,	
  Where_LD String, 
  Validation_OD String, 
  Where_OD String,
  Comment String
)
row format delimited fields terminated by "\u0059"
stored as textfile
tblproperties("skip.header.line.count"="1");
alter table custom_cb_k7m.S2T_Stage set serdeproperties ('serialization.null.format' = '');
load data local inpath 'S2T-Stage.csv' overwrite into table custom_cb_k7m.S2T_Stage;