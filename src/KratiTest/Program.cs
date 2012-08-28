using System;
using System.IO;

namespace KratiTest
{
    class Program
    {
        public static void Main(string[] args)
        {
            try
            {
                var dataDir = "C:\\krati\\testkratidb";//the folder where we store the data files.

                //lets delete the old test data if we have such a dir
                if (Directory.Exists(dataDir))
                    Directory.Delete(dataDir, true);

                // Create an instance of Krati DataStore
                using (var store = new KratiDataStore(dataDir, 1000000))
                {
                    // Populate data store
                    store.PopulateWithTestData();

                    // Perform some random reads from data store.
                    store.DoRandomReads(10);
                }
            }
            catch (Exception e)
            {
                Console.Write(e.StackTrace);
            }
        }
    }
}
