namespace BusterWood.Repositories.InformationSchema
{

    public class ColumnSchema
    {
        public string ColumnName { get; set; }
        public int OrdinalPosition { get; set; }
        public string ColumnDefault { get; set; }
        public string IsNullable { get; set; }
        public string DataType { get; set; }
        public int CharacterMaximumLength { get; set; }
        public int CharacterOctetLength { get; set; }
        public int NumericPrecision { get; set; }
        public int NumericPrecisionRadix { get; set; }
        public int NumericScale { get; set; }
        public bool IsIdentity { get; set; }

        string clrName;

        public string ClrName
        {
            get
            {
                if (clrName == null)
                    clrName = ColumnName.ToPascalCase();
                return clrName;
            }
        }
    }
}