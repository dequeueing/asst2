#include "tasksys.h"
#include <thread>
#include <atomic>

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
    size_t tasks_per_thread = num_total_tasks / num_threads_;
    size_t remaining_tasks = num_total_tasks % num_threads_;
    size_t current_task_id = 0;

    // Step0.1: prepare queue of workers
    std::vector<std::thread> workers;

    // Step1: spawn threads 
    for (size_t i = 0; i < num_threads_; ++i) {
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
    should_exit_ = false;
    has_work_ = false;
    
    // Create worker threads
    for (int i = 0; i < num_threads_; i++) {
        worker_threads_.emplace_back(&TaskSystemParallelThreadPoolSpinning::worker_thread_function, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // Signal threads to exit
    should_exit_ = true;
    
    // Wait for all threads to finish
    for (auto& thread : worker_threads_) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // Set up the work for worker threads
    current_runnable_ = runnable;
    total_tasks_ = num_total_tasks;
    next_task_id_ = 0;
    completed_tasks_ = 0;
    
    // Signal that work is available
    has_work_ = true;
    
    // Wait for all tasks to complete (spinning)
    while (completed_tasks_.load() < num_total_tasks) {
    }
    
    // Clear work flag
    has_work_ = false;
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

void TaskSystemParallelThreadPoolSpinning::worker_thread_function() {
    while (true) {
        // Check if we should exit
        if (should_exit_) {
            break;
        }
        
        // Check if there's work to do
        if (has_work_) {
            // Try to get a task
            int task_id = next_task_id_.fetch_add(1);
            
            // Check if we got a valid task
            if (task_id < total_tasks_) {
                // Execute the task
                current_runnable_->runTask(task_id, total_tasks_);
                
                // Mark this task as completed
                completed_tasks_.fetch_add(1);
            }
        }
        // Continue spinning (checking for work)
    }
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
