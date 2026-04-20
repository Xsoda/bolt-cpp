使用\ ``C++``\ 按原逻辑重写\ ``boltdb``\ .

.. code:: c++

   #include "bolt/bolt.hpp"

   template <typename Container>
   constexpr std::span<const std::byte> to_bytes(const Container &container) {
       return std::span<const std::byte>(
           reinterpret_cast<const std::byte *>(container.data()),
           container.size());
   }

   int main(int argc, char **argv) {
       bolt::DB db;
       if (auto err = db.Open("cache"); err != bolt::Success) {
           return -1;
       }
       if (auto err = db.Update([](bolt::Tx tx) -> bolt::ErrorCode {
           std::string bucket = "bucket";
           std::string key = "key";
           std::string value = "value";
           auto [b, err] = tx.CreateBucketIfNotExists(to_bytes(bucket));
           if (err != bolt::Success) {
               return err;
           }
           return b.Put(to_bytes(key), to_bytes(value));
       }); err != bolt::Success) {
           return -1;
       }
       db.Close();
       return 0;
   }

.. note::

   编译

   - Linux: gcc 11.4.0

   - Windows: Visual Studio 2022
