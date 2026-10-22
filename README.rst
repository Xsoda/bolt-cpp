使用\ ``C++``\ 按原逻辑重写\ ``boltdb``\ .

.. code:: c++

   #include "bolt/bolt.hpp"

   int main(int argc, char **argv) {
       bolt::DB db;
       if (auto err = db.Open("cache"); err != bolt::ErrorCode::Success) {
           return -1;
       }
       if (auto err = db.Update([](bolt::Tx tx) -> bolt::ErrorCode {
           std::string bucket = "bucket";
           std::string key = "key";
           std::string value = "value";
           auto [b, err] = tx.CreateBucketIfNotExists(bolt::to_bytes(bucket));
           if (err != bolt::ErrorCode::Success) {
               return err;
           }
           return b.Put(bolt::to_bytes(key), bolt::to_bytes(value));
       }); err != bolt::ErrorCode::Success) {
           return -1;
       }
       db.Close();
       return 0;
   }

.. note::

   编译

   - Linux: gcc 11.4.0

   - Windows: Visual Studio 2022


枚举数据
------------------

.. code:: c++

   db.View([](bolt::Tx tx) -> bolt::ErrorCode {
       auto cursor = tx.Cursor();
       auto [key, value] = cursor.First();
       while (!key.empty()) {
           // 不能以value.empty()来判断是否是bucket，因为键值对的值允许长度为0
           if (value.data() == nullptr) {
               fmt::println("bucket: {}", key);
               // auto bucket = tx.Bucket(key);
           } else {
               fmt::println("key-value: {} => {}", key, value);
           }
           std::tie(key, value) = cursor.Next();
       }
       return bolt::ErrorCode::Success;
   })
