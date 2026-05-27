using System;
using System.Collections.Generic;
using System.Text;

namespace Lotta.Tests
{
    public static class TestRun
    {
        private static string? _runRoot;

        public static string GetTempFolder()
        {
            if (_runRoot == null)
            {
                var baseDir = Path.Combine(Path.GetTempPath(), "LottaTests");
                _runRoot = Path.Combine(baseDir, $"run-{DateTime.Now:yyyyMMdd-HHmmss}");
                System.IO.Directory.CreateDirectory(_runRoot);

                // Keep only the 2 most recent runs
                if (System.IO.Directory.Exists(baseDir))
                {
                    var oldRuns = System.IO.Directory.GetDirectories(baseDir, "run-*")
                        .OrderByDescending(d => d)
                        .Skip(2)
                        .ToList();
                    foreach (var old in oldRuns)
                        try { System.IO.Directory.Delete(old, true); } catch { }
                }
            }
            return _runRoot;
        }
    }
}
