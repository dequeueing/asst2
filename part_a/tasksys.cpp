#include "tasksys.h"
#include <thread>
#include <mutex>
#include <memory>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), 
num_threads_(num_threads) 
{
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // Step0: decide dynamic or static assignment of tasks to threads
    // Step0.0: use a static methods, each thread handles num_total_tasks / num_threads task
    int tasks_per_thread = num_total_tasks / num_threads_;
    int remaining_tasks = num_total_tasks % num_threads_;
    int current_task_id = 0;

    // Step0.1: prepare queue of workers
    std::vector<std::thread> workers;

    // Step1: spawn threads 
    for (int i = 0; i < num_threads_; ++i) {
        size_t num_task_for_this_thread = tasks_per_thread;
        // evenly distrubute the remaining tasks to the first several threads
        if (i < remaining_tasks) {
            num_task_for_this_thread++;
        }

        // Spawn a thread and push it into the vector
        workers.emplace_back([=] {
            for (size_t task_id = current_task_id; task_id < current_task_id + num_task_for_this_thread; task_id += 1) {
                runnable->runTask(task_id, num_total_tasks);
            }
        });

        // Update the current task id
        current_task_id += num_task_for_this_thread;
    }

    // Step2: join all threads
    for (auto& t : workers) {
        t.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): 
    ITaskSystem(num_threads),
    num_threads_(num_threads),  
    all_tasks_enqueued_(false) 
{
    // construct the thread pool
    for (int i = 0; i < num_threads; ++i) {
        thread_pool_.emplace_back([this] {
            // declare a default task
            IRunnableWrapper task(nullptr, -1, 0);
            while (true) {
                {
                    // hold the lock for the shared queue
                    std::lock_guard<std::mutex> lock(this->lock_);
                    
                    // Check whether all tasks enqueued and finished
                    if (this->all_tasks_enqueued_ && this->tasks_.empty()) {
                        return;
                    }
                    
                    // Checkpoint: either queue is not empty or queue is empty but there are more to come

                    // Get a task if not empty
                    if (!this->tasks_.empty()) {
                        task = this->tasks_.front();
                        this->tasks_.pop(); // Remove the task from the queue
                    } 
                } // Modification to shared queue completed, can release the lock
                
                // Check whether a task is retrieved
                if (task.runnable_ptr) {
                    // printf("Executing task: %d\n", task.task_id);
                    task.runnable_ptr->runTask(task.task_id, task.num_total_tasks);
                    // printf("Task %d finished\n", task.task_id);
                } 
            }
        });
    }
    // printf("Thread pool constructed\n");
}
    

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // Main thread executes here
    for (int task_id = 0; task_id < num_total_tasks; task_id+=1) {
        // try acquire the lock
        {
            // Lock is acquired 
            std::lock_guard<std::mutex> lock(lock_);

            // Push the object into the shared queue
            IRunnableWrapper new_task(runnable, task_id, num_total_tasks);
            tasks_.emplace(std::move(new_task));
        } // Lock is released here!
    }
    all_tasks_enqueued_ = true;

    // Wait for all threads to join 
    for (auto& t : thread_pool_) {
        t.join();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
