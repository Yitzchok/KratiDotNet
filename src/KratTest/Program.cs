using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ikvm.extensions;
using krati.core;
using krati.core.segment;
using krati.store;
using Exception = System.Exception;
using File = java.io.File;
using String = System.String;

namespace KratTest
{
    class KratiDataStore
    {
        private readonly int itemsToInsert;
        private readonly StaticDataStore store;

        /**
         * Constructs KratiDataStore.
         * 
         * @param homeDir           the home directory of DataStore.
         * @param initialCapacity   the initial capacity of DataStore.
         * @throws Exception if a DataStore instance can not be created.
         */
        public KratiDataStore(File homeDir, int initialCapacity, int itemsToInsert)
        {
            this.itemsToInsert = itemsToInsert;
            store = CreateDataStore(homeDir, initialCapacity);
        }

        /**
         * @return the underlying data store.
         */
        public StaticDataStore DataStore()
        {
            return store;
        }

        /**
         * Creates a DataStore instance.
         * <p>
         * Subclasses can override this method to provide a specific DataStore implementation
         * such as DynamicDataStore and IndexedDataStore or provide a specific SegmentFactory
         * such as ChannelSegmentFactory, MappedSegmentFactory and WriteBufferSegment.
         */
        protected StaticDataStore CreateDataStore(File homeDir, int initialCapacity)
        {
            var config = new StoreConfig(homeDir, initialCapacity);
            config.setSegmentFactory(new MemorySegmentFactory());
            config.setSegmentFileSizeMB(32);
            
            return StoreFactory.createStaticDataStore(config);
        }

        /**
         * Creates data for a given key.
         * Subclasses can override this method to provide specific values for a given key.
         */
        protected byte[] CreateDataForKey(String key)
        {
            return ("Here is your data for " + key).getBytes();
        }

        /**
         * Populates the underlying data store.
         * 
         * @throws Exception
         */
        public void PopulateWithTestData()
        {
            Parallel.For(0, itemsToInsert, x =>
                {
                    string str = "key." + x;
                    byte[] key = str.getBytes();
                    byte[] value = CreateDataForKey(str);
                    store.put(key, value);
                });
            store.sync();
        }

        /**
         * Perform a number of random reads from the underlying data store.
         * 
         * @param readCnt the number of reads
         */
        public void DoRandomReads(int readCnt)
        {
            Random rand = new Random();
            for (int i = 0; i < readCnt; i++)
            {
                int keyId = rand.Next(itemsToInsert);
                String str = "key." + keyId;
                byte[] key = str.getBytes();
                byte[] value = store.get(key);
                System.Console.WriteLine("Key={0}\tValue={1}" + Environment.NewLine, str, Encoding.UTF8.GetString(value));
            }
        }

        /**
         * Checks if the <code>KratiDataStore</code> is Open for operations.
         */
        public bool IsOpen()
        {
            return store.isOpen();
        }

        /**
         * Opens the store.
         * 
         * @throws IOException
         */
        public void Open()
        {
            store.open();
        }

        /**
         * Closes the store.
         * 
         * @throws IOException
         */
        public void Close()
        {
            store.close();
        }

        public static void Main(string[] args)
        {
            try
            {
                // Parse arguments: keyCount homeDir
                var dbDir = "C:\\krati\\testdb";
                if (Directory.Exists(dbDir))
                    Directory.Delete(dbDir, true);
                File homeDir = new File(dbDir);
                int initialCapacity = 1000000;

                // Create an instance of Krati DataStore
                var store = new KratiDataStore(homeDir, initialCapacity, 1000000);

                // Populate data store
                //Parallel.For(0, 1000, x => store.PopulateWithTestData());
                store.PopulateWithTestData();
                // Perform some random reads from data store.
                store.DoRandomReads(10);

                // Close data store
                store.Close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
}
