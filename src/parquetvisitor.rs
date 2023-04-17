use parquet::{
    basic::{ConvertedType, Repetition},
    errors::{ParquetError::General, Result},
    schema::types::{Type, TypePtr},
};

/// Parquet schema visitor
///
/// Based on <https://docs.rs/parquet/latest/parquet/schema/visitor/trait.TypeVisitor.html>
pub trait ParquetTypeVisitor<R, C> {
    /// Called when a primitive type hit.
    fn visit_primitive(&mut self, primitive_type: TypePtr, context: C) -> Result<R>;

    /// Default implementation when visiting a list.
    ///
    /// It checks list type definition and calls [`Self::visit_list_with_item`] with extracted
    /// item type.
    ///
    /// To fully understand this algorithm, please refer to
    /// [parquet doc](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md).
    ///
    /// For example, a standard list type looks like:
    ///
    /// ```text
    /// required/optional group my_list (LIST) {
    //    repeated group list {
    //      required/optional binary element (UTF8);
    //    }
    //  }
    /// ```
    ///
    /// In such a case, [`Self::visit_list_with_item`] will be called with `my_list` as the list
    /// type, and `element` as the `item_type`
    ///
    fn visit_list(&mut self, list_type: TypePtr, context: C) -> Result<R> {
        match list_type.as_ref() {
            Type::PrimitiveType { .. } => {
                panic!("{list_type:?} is a list type and must be a group type")
            }
            Type::GroupType {
                basic_info: _,
                fields,
            } if fields.len() == 1 => {
                let list_item = fields.first().unwrap();

                match list_item.as_ref() {
                    Type::PrimitiveType { .. } => {
                        if list_item.get_basic_info().repetition() == Repetition::REPEATED {
                            self.visit_list_with_item(list_type.clone(), list_item.clone(), context)
                        } else {
                            Err(General(
                                "Primitive element type of list must be repeated.".to_string(),
                            ))
                        }
                    }
                    Type::GroupType {
                        basic_info: _,
                        fields,
                    } => {
                        if fields.len() == 1
                            && list_item.name() != "array"
                            && list_item.name() != format!("{}_tuple", list_type.name())
                        {
                            self.visit_list_with_item(
                                list_type.clone(),
                                fields.first().unwrap().clone(),
                                context,
                            )
                        } else {
                            self.visit_list_with_item(list_type.clone(), list_item.clone(), context)
                        }
                    }
                }
            }
            _ => Err(General(
                "Group element type of list can only contain one field.".to_string(),
            )),
        }
    }

    /// Called when a struct type hit.
    fn visit_struct(&mut self, struct_type: TypePtr, context: C) -> Result<R>;

    /// Called when a map type hit.
    fn visit_map(&mut self, map_type: TypePtr, context: C) -> Result<R>;

    /// A utility method which detects input type and calls corresponding method.
    fn dispatch(&mut self, cur_type: TypePtr, context: C) -> Result<R> {
        if cur_type.is_primitive() {
            self.visit_primitive(cur_type, context)
        } else {
            let basic_info = cur_type.get_basic_info();
            let name = basic_info.name();
            let converted_type = basic_info.converted_type();
            if converted_type == ConvertedType::LIST {
                self.visit_list(cur_type, context)
            } else if converted_type == ConvertedType::MAP
                || converted_type == ConvertedType::MAP_KEY_VALUE
                || name == "key_value"
            {
                self.visit_map(cur_type, context)
            } else {
                self.visit_struct(cur_type, context)
            }
        }
    }

    /// Called by `visit_list`.
    fn visit_list_with_item(
        &mut self,
        list_type: TypePtr,
        item_type: TypePtr,
        context: C,
    ) -> Result<R>;
}
