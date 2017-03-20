using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BusterWood.Repositories
{
    static class Strings
    {
        public static string ToPascalCase(this string text)
        {
            var temp = new StringBuilder(text.Length);
            char prev = '_';
            foreach (char ch in text)
            {
                if (prev == '_')
                    temp.Append(char.ToUpper(ch));
                else if (ch != '_')
                    temp.Append(char.ToLower(ch));
                prev = ch;
            }
            return temp.ToString();
        }
    }
}
