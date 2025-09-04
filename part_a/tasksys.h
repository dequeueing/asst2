#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <queue>
#include <memory>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        int num_threads_;
};

struct IRunnableWrapper {
    IRunnable* runnable_ptr;
    int task_id;
    int num_total_tasks;

    // Constructor to create a Job
    IRunnableWrapper(IRunnable* runnable_ptr, int t_id, int total_tasks):
        runnable_ptr(runnable_ptr), 
        task_id(t_id),
        num_total_tasks(total_tasks)
    {}
};


/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        int num_threads_;                         // static, read-only across threads
        std::atomic<bool> threads_should_quit_;   // indicate the system deallocated
        std::atomic<int> tasks_completed_;        // for the current run(), how many tasks have been completed by workers
        
        std::mutex lock_;                       // lock for shared queue 
        std::queue<IRunnableWrapper> tasks_;    // queue of tasks
        std::vector<std::thread> thread_pool_;  // thread pool
};


/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        std::vector<std::thread> thread_pool_;    // thread pool        
        int num_threads_;                         // static for the whole system lifetime 
        int num_tasks_;                           // static for the lifetime of a run()
        
        std::atomic<bool> system_quit_;   // indicate the system is deallocated, all threads should quit and join
        
        bool system_have_tasks_;            // system.run() is called
        std::mutex lock_system_have_tasks_;  // lock for system_have_tasks_
        std::condition_variable cv_system_have_tasks_;   // sleep worker threads when there are no tasks in the systems

        int num_tasks_completed_;             // number of tasks already finished by the workre threads
        std::mutex lock_num_tasks_completed_;  // lock for num_tasks_completed_
        std::condition_variable cv_num_tasks_completed_;  // sleep main thread when waiting for worker threads

        std::atomic<int> current_task_id_;           // atomic variable to keep track of current task id

        // thread worker function
        void worker_thread_function();

        IRunnable* task_runnable_; // task runnable
};

#endif
