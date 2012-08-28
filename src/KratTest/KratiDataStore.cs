using System;
using System.Text;
using System.Threading.Tasks;
using ikvm.extensions;
using java.io;
using krati.core;
using krati.core.segment;
using krati.store;

namespace KratTest
{
    public class KratiDataStore : IDisposable
    {
        private readonly int itemsToInsert;
        private readonly StaticDataStore store;

        /// <param name="dbDir">the home directory of DataStore.</param>
        /// <param name="initialCapacity">the initial capacity of DataStore.</param>
        public KratiDataStore(string dbDir, int initialCapacity)
        {
            this.itemsToInsert = initialCapacity;
            store = CreateDataStore(new File(dbDir), initialCapacity);
        }

        public StaticDataStore DataStore()
        {
            return store;
        }

        /// <summary>
        /// Creates a DataStore instance.
        /// <p>
        /// Subclasses can override this method to provide a specific DataStore implementation
        /// such as DynamicDataStore and IndexedDataStore or provide a specific SegmentFactory
        /// such as ChannelSegmentFactory, MappedSegmentFactory and WriteBufferSegment.
        /// </summary>
        /// <param name="homeDir">the home directory of DataStore.</param>
        /// <param name="initialCapacity">the initial capacity of DataStore.</param>
        /// <returns>The data store</returns>
        protected StaticDataStore CreateDataStore(File homeDir, int initialCapacity)
        {
            var config = new StoreConfig(homeDir, initialCapacity);
            config.setSegmentFactory(new MemorySegmentFactory());
            config.setSegmentFileSizeMB(32);

            return StoreFactory.createStaticDataStore(config);
        }

        /// <summary>
        /// Creates data for a given key.
        /// Subclasses can override this method to provide specific values for a given key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        protected byte[] CreateDataForKey(String key)
        {
            return ("Here is your data for " + key).getBytes();
        }

        /// <summary>
        /// Populates the underlying data store with test data.
        /// </summary>
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

        /// <summary>
        /// Perform a number of random reads from the underlying data store.
        /// </summary>
        /// <param name="readCnt">readCnt the number of reads</param>
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

        public void Dispose()
        {
            store.close();
        }
    }
}