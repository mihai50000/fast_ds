#ifndef DS_LOCKFREEARRAY_H
#define DS_LOCKFREEARRAY_H

#include <algorithm>
#include <cstddef>

namespace fast_ds {

    namespace internal {
        template<class T>
        class write_descriptor {
        public:
            T old_value_;
            T new_value_;
            size_t location_;
            bool completed_;

            write_descriptor() : location_(0), completed_(true) {}

            explicit write_descriptor(T oldValue, T newValue, size_t location, bool completed) : old_value_(oldValue),
                                                                                                 new_value_(newValue),
                                                                                                 location_(location),
                                                                                                 completed_(completed) {}
        };

        template<class T>
        class v_descriptor {
        public:
            size_t size_ = 0;
            write_descriptor<T> writeDescriptor_;

            v_descriptor() = default;

            explicit v_descriptor(size_t size, write_descriptor<T> w_descriptor) : size_(size),
                                                                                   writeDescriptor_(w_descriptor) {}
        };
    }

    template<class T>
    class LockFreeArray {
    private:
        static constexpr size_t kNumberOfBuckets = 32;
        static constexpr size_t kFirstBucketCapacity = 2;

        std::atomic<std::atomic<T> *> *data_;
        std::shared_ptr<internal::v_descriptor<T>> descriptor_ = std::make_shared<internal::v_descriptor<T>>();

        void CompleteWrite(internal::write_descriptor<T> write_descriptor) {
            if (!write_descriptor.completed_) {
                std::atomic<T> *memoryLocation = At(write_descriptor.location_);
                memoryLocation->compare_exchange_weak(write_descriptor.old_value_,
                                                      write_descriptor.new_value_);

                write_descriptor.completed_ = true;
            }
        }

        inline int HighestBitSet(unsigned int x) const {
            return 31 - __builtin_clz(x);
        }

        void AllocBucket(int bucket) {
            int bucketSize = 1 << (bucket + HighestBitSet(kFirstBucketCapacity));
            std::atomic<T> *currentBucket = data_[bucket].load();
            auto newBucket = new std::atomic<T>[bucketSize];

            if (!data_[bucket].compare_exchange_weak(currentBucket, newBucket)) {
                delete[] newBucket;
            }
        }

    public:
        explicit LockFreeArray() {
            data_ = new std::atomic<std::atomic<T> *>[kNumberOfBuckets];
            data_[0].store(new std::atomic<T>[kFirstBucketCapacity]);
        }

        LockFreeArray(const LockFreeArray &other) = delete;

        size_t Size() noexcept {
            auto current_descriptor = descriptor_;
            auto current_size = current_descriptor->size_;

            if (!descriptor_->writeDescriptor_.completed_) {
                current_size--;
            }

            return current_size;
        }

        inline bool Empty() const noexcept {
            return Size() == 0;
        }

        std::atomic<T> *At(size_t index) const {
            auto pos = index + kFirstBucketCapacity;
            auto hibit = HighestBitSet(pos);
            auto index_in_bucket = (pos ^ (1 << hibit));
            return &data_[hibit - HighestBitSet(kFirstBucketCapacity)][index_in_bucket];
        }

        void PushBack(const T &value) {
            internal::v_descriptor<T> *current_descriptor;
            std::shared_ptr<internal::v_descriptor<T>> new_descriptor;
            std::shared_ptr<internal::v_descriptor<T>> current_descriptor_ptr = descriptor_;

            do {
                current_descriptor = descriptor_.get();
                if (current_descriptor == nullptr) {
                    continue;
                }
                current_descriptor_ptr = descriptor_;
                int current_size = current_descriptor->size_;
                internal::write_descriptor<T> wDescriptor = current_descriptor->writeDescriptor_;
                CompleteWrite(wDescriptor);

                if (int bucket = HighestBitSet(current_size + kFirstBucketCapacity) -
                                 HighestBitSet(kFirstBucketCapacity); data_[bucket].load() == 0) {
                    AllocBucket(bucket);
                }

                auto oldValue = At(current_size)->load();
                auto write_op = internal::write_descriptor<T>(oldValue, value,
                                                              current_size,
                                                              false);

                new_descriptor = std::make_shared<internal::v_descriptor<T>>(
                        internal::v_descriptor(current_size + 1, write_op));
            } while (!std::atomic_compare_exchange_strong(&descriptor_, &current_descriptor_ptr, new_descriptor));
            CompleteWrite(new_descriptor->writeDescriptor_);
        }

        T PopBack() {
            internal::v_descriptor<T> *current_descriptor;
            auto descriptorPtr = descriptor_;
            auto new_descriptor = std::make_shared<internal::v_descriptor<T>>();

            T elem;
            do {
                current_descriptor = descriptor_.get();
                if (current_descriptor == nullptr) {
                    continue;
                }
                descriptorPtr = descriptor_;
                int current_size = current_descriptor->size_;
                internal::write_descriptor<T> wDescriptor = current_descriptor->writeDescriptor_;
                CompleteWrite(wDescriptor);

                if (current_size == 0) {
                    throw std::out_of_range("vector is Empty");
                }

                elem = At(current_size - 1)->load();
                *new_descriptor = internal::v_descriptor(current_size - 1, internal::write_descriptor<T>());
            } while (!std::atomic_compare_exchange_weak(&descriptor_, &descriptorPtr, new_descriptor));

            return elem;
        }

        ~LockFreeArray() {
            //TODO
        }
    };
}

#endif
