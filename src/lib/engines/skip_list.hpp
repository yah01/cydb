#pragma once

#include <memory>
#include <iterator>
#include <vector>
#include <list>
#include <algorithm>

namespace cyber
{
    template <class T>
    class SkipList
    {
    public:
        struct Node
        {
            T data;
            Node *prev;
            Node *next;
            Node *down;

            Node() = delete;

            Node(const T &data,
                 Node *prev = nullptr,
                 Node *next = nullptr,
                 Node *down = nullptr) : data(data),
                                         prev(prev),
                                         next(next),
                                         down(down) {}

            Node(T &&data,
                 Node *prev = nullptr,
                 Node *next = nullptr,
                 Node *down = nullptr) : data(std::move(data)),
                                         prev(prev),
                                         next(next),
                                         down(down) {}
        };

        class iterator : public std::iterator<std::random_access_iterator_tag,
                                              T,
                                              int32_t,
                                              T *,
                                              T &>
        {
            Node *prev;
            Node *cur;

        public:
            explicit iterator(const Node *prev, const Node *cur) : prev(prev), cur(cur) {}
            T &operator*() const
            {
                std::list<int> list;
                return cur->data;
            }

            iterator &operator++()
            {
                if (cur == nullptr)
                    return *this;

                prev = cur;
                cur = cur->next;
                return *this;
            }

            iterator operator++(int)
            {
                if (cur == nullptr)
                    return *this;

                iterator tmp = *this;
                prev = cur;
                cur = cur->next;
                return tmp;
            }

            iterator &operator--()
            {
                if (prev == nullptr) // it's the first element
                    return *this;

                cur = prev;
                prev = prev->prev;
                return *this;
            }

            iterator &operator--(int)
            {
                if (prev == nullptr) // it's the first element
                    return *this;

                iterator tmp = *this;
                cur = prev;
                prev = prev->prev;
                return tmp;
            }

            bool operator==(const iterator &rhs) { return cur == rhs.cur; }
            bool operator!=(const iterator &rhs) { return cur != rhs.cur; }
        };

        using node_list = std::list<Node>;

        iterator insert(const T &val)
        {
            if (levels.empty())
                levels.push_front(node_list());

            if (auto it = std::find_if(levels.rbegin(), levels.rend(), [&val](const node_list &list) { return list.front().data <= val; });
                it == levels.rend())
            {
                levels.front().push_front(Node(val));
            }            
        }

        iterator lower_bound(const T &val)
        {
        }

        iterator begin() { return levels.empty() ? iterator(nullptr, nullptr) : iterator(nullptr, levels.front().head); }
        iterator end() { return levels.empty() ? iterator(nullptr, nullptr) : iterator(levels.front().tail, nullptr); }

    private:
        const static int p = 2;
        struct LevelHead
        {

            Node *head, *tail;
        };

        std::list<node_list> levels;

    }; // class SkipList
} // namespace cyber