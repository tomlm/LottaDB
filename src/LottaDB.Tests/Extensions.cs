namespace Lotta.Tests
{
    public static class Extensions
    {
        private static readonly string _runRoot;

        static Extensions()
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

        public static LottaCatalog ConfigureTestStorage(this LottaCatalog catalog, string provider = "Memory")
        {
            switch (provider)
            {
                case "FileSystem": catalog.UseFileSystem(_runRoot); 
                    break;
                case "SQLite": catalog.UseSQLite(_runRoot); 
                    break;
                case "Azurite": catalog.UseAzure("UseDevelopmentStorage=true"); 
                    break;
                case "Memory":
                default:
                    catalog.UseMemory(); 
                    break;
            }
            return catalog;
        }

    }
}
