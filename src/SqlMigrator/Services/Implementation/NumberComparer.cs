using System;
using System.Text.RegularExpressions;
using SqlMigrator.Model;

namespace SqlMigrator.Services.Implementation
{
    public class NumberComparer : ICompareMigrations
    {
        private static readonly Regex Numbers = new Regex(@"\d+");

        public int Compare(Migration x, Migration y)
        {
            var matchesX = GetMatches(x.Number);
            var matchesY = GetMatches(y.Number);
            for (var i = 0; i < matchesX.Count; i++)
            {
                if (matchesY.Count <= i)
                {
                    //Since the other version number has less elements and so far they are equals, then this MigrationX is greater then MigrationY
                    return 1;
                }
                var numberX = long.Parse(matchesX[i].Value);
                var numberY = long.Parse(matchesY[i].Value);
                var result = numberX.CompareTo(numberY);
                if (result != 0)
                {
                    return result;
                }
            }
            if (matchesY.Count > matchesX.Count)
            {
                return -1;
            }
            return 0;
        }

        public bool IsMigrationAfterVersion(Migration m, string version)
        {
            return Compare(m, new Migration {Number = version}) == 1;
        }

        private MatchCollection GetMatches(string s)
        {
            var retval = Numbers.Matches(s);
            if (retval.Count == 0)
            {
                throw new FormatException(string.Format("invalid version format for this comparer : {0}", s));
            }
            return retval;
        }
    }
}