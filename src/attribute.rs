pub enum AttributeType {
    Int,
    Real,
    Varchar { len: u32 },
}

pub struct Attribute {
    pub name: String,
    pub attribute_type: AttributeType,
}
#[derive(Debug, PartialEq)]
pub enum AttributeValue {
    Int(i32),
    Real(f64),
    Varchar(String),
}
