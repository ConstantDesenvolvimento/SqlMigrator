using System;
using NUnit.Framework;
using SqlMigrator;


namespace UnitTests.Facts
{
    [TestFixture]
    public class NumberComparerFacts
    {
        private readonly object[] _xLessThenY =
        {
            new object[] {"1", "2"},
            new object[] {"1.1", "1.2"},
            new object[] {"20150101", "20150102"},
            new object[] {"20150101-01", "20150102-01"},
            new object[] {"20150101-01", "20150101-02"},
            new object[] {"2015.01.01", "2015.01.01-02"},

        };
        private readonly object[] _xGreaterThenY =
        {
            new object[] {"2", "1"},
            new object[] {"1.2", "1.1"},
            new object[] {"20150102", "20150101"},
            new object[] {"20150102-01", "20150101-01"},
            new object[] {"20150101-02", "20150101-01"},
            new object[] {"2015.01.01-01", "2015.01.01"}

        };
        private readonly object[] _xEqualsY =
        {
            new object[] {"1", "1"},
            new object[] {"1.1", "1.1"},
            new object[] {"20150101", "20150101"},
            new object[] {"20150101-01", "20150101-01"},
            new object[] {"2015.01.01-01", "2015/01/01-01"},

        };
        [TestCaseSource("_xLessThenY")]
        public void compare_x_less_then_y(string x,string y)
        {
            var migrationA = new Migration {Number = x};
            var migrationB = new Migration {Number = y};
            var comparer = new NumberComparer();
            Assert.AreEqual(-1, comparer.Compare(migrationA, migrationB));
        }
        [TestCaseSource("_xEqualsY")]
        public void compare_x_equals_y(string x, string y)
        {
            var migrationA = new Migration { Number = x };
            var migrationB = new Migration { Number = y };
            var comparer = new NumberComparer();
            Assert.AreEqual(0, comparer.Compare(migrationA, migrationB));
        }
        [TestCaseSource("_xGreaterThenY")]
        public void compare_x_greaterThen_y(string x, string y)
        {
            var migrationA = new Migration { Number = x };
            var migrationB = new Migration { Number = y };
            var comparer = new NumberComparer();
            Assert.AreEqual(1, comparer.Compare(migrationA, migrationB));
        }
        [TestCaseSource("_xGreaterThenY")]
        public void compare_x_greaterThen_version_y(string x, string y)
        {
            var migrationA = new Migration { Number = x };
            var comparer = new NumberComparer();
            Assert.AreEqual(true, comparer.IsMigrationAfterVersion(migrationA, y));
        }
        [TestCaseSource("_xEqualsY")]
        public void compare_x_not_greater_then_version_y_equals(string x, string y)
        {
            var migrationA = new Migration { Number = x };
            var comparer = new NumberComparer();
            Assert.AreEqual(false, comparer.IsMigrationAfterVersion(migrationA, y));
        }
        [TestCaseSource("_xLessThenY")]
        public void compare_x_not_greater_then_version_y_lesser(string x, string y)
        {
            var migrationA = new Migration { Number = x };
            var comparer = new NumberComparer();
            Assert.AreEqual(false, comparer.IsMigrationAfterVersion(migrationA, y));
        }

        [Test]
        public void InvalidFormatX()
        {
            var migrationA = new Migration { Number = "ABC" };
            var migrationB = new Migration { Number = "111" };
            var comparer = new NumberComparer();
            Assert.Throws<FormatException>(() => Assert.AreNotEqual(0, comparer.Compare(migrationA, migrationB)));

        }
        [Test]
        public void InvalidFormatY()
        {
            var migrationA = new Migration { Number = "111" };
            var migrationB = new Migration { Number = "ABC" };
            var comparer = new NumberComparer();
            Assert.Throws<FormatException>(() => Assert.AreNotEqual(0, comparer.Compare(migrationA, migrationB)));

        }
    }
}