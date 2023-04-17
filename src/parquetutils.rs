use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use parquet::{
    basic::{ConvertedType, Type},
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
};

use crate::parquetvisitor::ParquetTypeVisitor;

pub struct ParquetUtils {}

impl ParquetUtils {
    pub fn parquet_schema_to_clickhouse<P: AsRef<Path>, S: Into<String>>(
        parquet_path: P,
        clickhouse_schema_path: P,
        table_name: S,
        primary_key: S,
    ) -> anyhow::Result<()> {
        let primary_key = primary_key.into();
        let table_name = table_name.into();

        match File::open(parquet_path) {
            Ok(file) => {
                let reader = SerializedFileReader::new(file)?;
                let metadata = reader.metadata();
                let schema = metadata.file_metadata().schema_descr();

                let mut visitor = ParquetVisitor::new(clickhouse_schema_path);
                visitor
                    .bw
                    .write_all(format!("drop table if exists {};\n", table_name).as_bytes())?;
                visitor
                    .bw
                    .write_all(format!("create table {} (\n", table_name).as_bytes())?;
                let fields = schema.root_schema().get_fields();
                for (i, field) in fields.iter().enumerate() {
                    let mut context = ParquetVisitorContext::new(&primary_key, 0);
                    context.indent = 4;
                    let indentation = " ".repeat(context.indent);
                    if i > 0 {
                        visitor
                            .bw
                            .write_all(format!("{}, ", indentation).as_bytes())?;
                    } else {
                        visitor.bw.write_all(indentation.as_bytes())?;
                    }
                    visitor.dispatch(field.clone(), context)?;
                }

                visitor.bw.write_all(
                    format!(") engine = MergeTree() primary key ({});\n", primary_key).as_bytes(),
                )?;
            }
            Err(e) => panic!("{}", e),
        }
        Ok(())
    }
}

//--------------------------------------------------------------------------------------------------

#[derive(Debug)]
pub enum ClickhouseType {
    Bool,
    Float32,
    Float64,
    Int32,
    Int64,
    String,
}

impl From<Type> for ClickhouseType {
    fn from(value: Type) -> Self {
        match value {
            Type::BOOLEAN => Self::Bool,
            Type::INT32 => Self::Int32,
            Type::INT64 => Self::Int64,
            Type::FLOAT => Self::Float32,
            Type::DOUBLE => Self::Float64,
            Type::BYTE_ARRAY => Self::String,
            _ => unimplemented!(),
        }
    }
}

impl From<ConvertedType> for ClickhouseType {
    fn from(value: ConvertedType) -> Self {
        match value {
            ConvertedType::UTF8 => Self::String,
            ConvertedType::DATE => Self::Int32,
            ConvertedType::TIMESTAMP_MILLIS => Self::Int64,
            other => unimplemented!("{}", other),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetVisitorContext {
    pub primary_key: String,
    pub indent: usize,
    pub field_name: String,
}

impl ParquetVisitorContext {
    pub fn new<S: Into<String>>(primary_key: S, indent: usize) -> Self {
        Self {
            primary_key: primary_key.into(),
            indent,
            field_name: "".into(),
        }
    }
}

pub struct ParquetVisitor {
    pub bw: BufWriter<File>,
}

impl ParquetVisitor {
    pub fn new<P: AsRef<Path>>(clickhouse_schema_path: P) -> Self {
        let file = std::fs::File::create(clickhouse_schema_path).unwrap();
        let bw = BufWriter::new(file);
        Self { bw }
    }

    fn converted_type_to_clickhouse(
        &self,
        converted_type: ConvertedType,
        physical_type: Type,
    ) -> ClickhouseType {
        if converted_type == ConvertedType::NONE {
            physical_type.into()
        } else {
            converted_type.into()
        }
    }
}

#[allow(unused)]
impl ParquetTypeVisitor<bool, ParquetVisitorContext> for ParquetVisitor {
    fn visit_primitive(
        &mut self,
        primitive_type: parquet::schema::types::TypePtr,
        context: ParquetVisitorContext,
    ) -> parquet::errors::Result<bool> {
        let basic_info = primitive_type.get_basic_info();
        let converted_type = basic_info.converted_type();
        let physical_type = primitive_type.get_physical_type();

        let tp = self.converted_type_to_clickhouse(converted_type, physical_type);

        if context.field_name == "Map" {
            // special handling of Map key
            self.bw.write_all(format!("{:?}\n", tp).as_bytes());
        } else {
            // TODO - special handling of primary key field
            if basic_info.name() == context.primary_key {
                self.bw
                    .write_all(format!("{} {:?}\n", basic_info.name(), tp).as_bytes());
            } else if basic_info.name() == "array" || basic_info.name() == "element" {
                // Special case of List with a primitive type (and not a Struct type)
                let indentation = " ".repeat(context.indent);
                self.bw.write_all(
                    format!("{}{} Nullable({:?})\n", indentation, basic_info.name(), tp).as_bytes(),
                );
            } else {
                self.bw
                    .write_all(format!("{} Nullable({:?})\n", basic_info.name(), tp).as_bytes());
            }
        }

        Ok(true)
    }

    fn visit_struct(
        &mut self,
        struct_type: parquet::schema::types::TypePtr,
        mut context: ParquetVisitorContext,
    ) -> parquet::errors::Result<bool> {
        let basic_info = struct_type.get_basic_info();

        let name = basic_info.name();
        if name != "array" && name != "list" && name != "element" && name != "item" {
            // we assume it's a Tuple or nested list struct
            context.indent += 4;
            if context.field_name == "Map" {
                // when the Map value is a Tuple
                context.field_name = "MapTupleValue".into();
                self.bw.write_all("Tuple(\n".as_bytes());
            } else {
                self.bw.write_all(format!("{} Tuple(\n", name).as_bytes());
            }
        }

        let indentation = if name != "list" {
            " ".repeat(context.indent)
        } else {
            "".repeat(context.indent)
        };

        let fields = struct_type.get_fields();
        for (i, field) in fields.iter().enumerate() {
            if i == 0 {
                self.bw.write_all(indentation.as_bytes());
            } else {
                self.bw.write_all(format!("{}, ", indentation).as_bytes());
            }
            self.dispatch(field.clone(), context.clone());
        }

        if name != "array" && name != "list" && name != "element" && name != "item" {
            context.indent -= 4;
            let indentation = " ".repeat(context.indent);
            self.bw.write_all(format!("{})\n", indentation).as_bytes());
        }

        Ok(true)
    }

    fn visit_map(
        &mut self,
        map_type: parquet::schema::types::TypePtr,
        mut context: ParquetVisitorContext,
    ) -> parquet::errors::Result<bool> {
        let basic_info = map_type.get_basic_info();

        if basic_info.converted_type() == ConvertedType::MAP {
            self.bw
                .write_all(format!("{} Map (\n", basic_info.name()).as_bytes());
        }

        let indentation = if basic_info.name() == "key_value" {
            context.indent += 4;
            " ".repeat(context.indent)
        } else {
            "".to_string()
        };

        let fields = map_type.get_fields();
        context.field_name = "Map".into();

        for (i, field) in fields.iter().enumerate() {
            if i == 0 {
                self.bw.write_all(indentation.as_bytes());
            } else {
                self.bw.write_all(format!("{}, ", indentation).as_bytes());
            }
            self.dispatch(field.clone(), context.clone());
        }

        if basic_info.name() == "key_value" {
            context.indent -= 4;
            let indentation = " ".repeat(context.indent);
            self.bw.write_all(format!("{})\n", indentation).as_bytes());
        }

        Ok(true)
    }

    fn visit_list_with_item(
        &mut self,
        list_type: parquet::schema::types::TypePtr,
        item_type: parquet::schema::types::TypePtr,
        mut context: ParquetVisitorContext,
    ) -> parquet::errors::Result<bool> {
        match list_type.get_basic_info().converted_type() {
            ConvertedType::LIST => {
                let basic_info = list_type.get_basic_info();

                let name = basic_info.name();
                let indentation = " ".repeat(context.indent);
                self.bw.write_all(format!("{} Nested ", name).as_bytes());
                self.bw.write_all("(\n".as_bytes());

                context.indent += 4;

                let fields = list_type.get_fields();
                for (i, field) in fields.iter().enumerate() {
                    self.dispatch(field.clone(), context.clone());
                }

                self.bw.write_all(format!("{})\n", indentation).as_bytes());
            }
            _ => unimplemented!(),
        }

        Ok(true)
    }
}

// -------------------------------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use std::{
        fs::File,
        io::{BufReader, Read},
        path::Path,
        sync::Arc,
    };

    use arrow::{
        array::{
            ArrayRef, GenericListBuilder, Int32Array, StringArray, StringBuilder, StructArray,
            StructBuilder,
        },
        datatypes::{DataType, Field, Fields},
        record_batch::RecordBatch,
    };
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use tempfile::TempDir;

    use super::ParquetUtils;

    #[test]
    fn test_parquet_schema_to_clickhouse() -> anyhow::Result<()> {
        let tmp_dir = TempDir::new()?;

        let batch = create_record_batch()?;

        let parquet_path = tmp_dir.path().join("p.parquet");
        let clickhouse_schema_path = tmp_dir.path().join("clickhouse_schema.sql");

        write_parquet(batch, &parquet_path)?;

        ParquetUtils::parquet_schema_to_clickhouse(
            parquet_path,
            clickhouse_schema_path.clone(),
            "T",
            "foo",
        )?;

        verify_schema(clickhouse_schema_path)?;

        tmp_dir.close()?;

        Ok(())
    }

    fn create_record_batch() -> anyhow::Result<RecordBatch> {
        // Primitive i32 array
        let a_array = Int32Array::from(vec![Some(42)]);

        // Primitive string array
        // let b_array = StringArray::from(vec![Some("ahmed")]);
        let _t: Vec<Option<String>> = vec![None];
        let b_array = StringArray::from(_t);

        // Struct array
        let f1 = Arc::new(StringArray::from(vec!["foo"]));
        let f2 = Arc::new(StringArray::from(vec!["bar"]));
        let c_struct = StructArray::from(vec![
            (Field::new("a", DataType::Utf8, true), f1 as ArrayRef),
            (Field::new("b", DataType::Utf8, true), f2 as ArrayRef),
        ]);

        // List of Struct
        let fields = Fields::from(vec![Field::new("a", DataType::Utf8, true)]);
        let mut values_builder = StructBuilder::new(fields, vec![Box::new(StringBuilder::new())]);
        let string_builder = values_builder.field_builder::<StringBuilder>(0).unwrap();
        string_builder.append_value("foo");
        values_builder.append(true);
        let mut builder = GenericListBuilder::<i32, _>::new(values_builder);
        builder.append(true);
        let list_array = builder.finish();

        // columns
        let a = Arc::new(a_array) as _;
        let b = Arc::new(b_array) as _;
        let c = Arc::new(c_struct) as _;
        let d = Arc::new(list_array) as _;

        let batch = RecordBatch::try_from_iter([("a", a), ("b", b), ("c", c), ("d", d)])?;
        Ok(batch)
    }

    fn write_parquet<P: AsRef<Path>>(batch: RecordBatch, parquet_path: P) -> anyhow::Result<()> {
        let file = File::create(parquet_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn verify_schema<P: AsRef<Path>>(clickhouse_schema_path: P) -> anyhow::Result<()> {
        let file = File::open(clickhouse_schema_path)?;
        let mut br = BufReader::new(file);

        let mut schema = String::new();
        br.read_to_string(&mut schema)?;

        // \x20 is used to preserve the spacing
        let expected_schema = "\
        drop table if exists T;\n\
        create table T (\n\
        \x20   a Nullable(Int32)\n\
        \x20   , b Nullable(String)\n\
        \x20   , c Tuple(\n\
        \x20       a Nullable(String)\n\
        \x20       , b Nullable(String)\n\
        \x20   )\n\
        \x20   , d Nested (\n\
        \x20       a Nullable(String)\n\
        \x20   )\n\
        ) engine = MergeTree() primary key (foo);\n\
        ";

        assert_eq!(expected_schema, schema);

        Ok(())
    }
}
