#include "tasksys.h"
#include <thread>
#include <mutex>
#include <memory>
#include <condition_variable>


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
    threads_should_quit_(false) 
{
    // construct the thread pool
    for (int i = 0; i < num_threads; ++i) {
        thread_pool_.emplace_back([this] {
            // declare a default task
            
            while (true) {
                // Check whether the system is deallocateds
                if (this->threads_should_quit_) {
                    return;
                }

                IRunnableWrapper task(nullptr, -1, 0);
                {
                    // hold the lock for the shared queue
                    std::lock_guard<std::mutex> lock(this->lock_);
                    
                    // Get a task if not empty
                    if (!this->tasks_.empty()) {
                        task = this->tasks_.front();
                        this->tasks_.pop(); // Remove the task from the queue
                    } 
                } // Modification to shared queue completed, can release the lock
                
                // Check whether a task is retrieved
                if (task.runnable_ptr) {
                    task.runnable_ptr->runTask(task.task_id, task.num_total_tasks);                    

                    // Task, finished, increament the counter 
                    tasks_completed_.fetch_add(1);
                } 
            }
        });
    }
    // printf("Thread pool constructed\n");
}
    

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // Flag all threads should quit 
    threads_should_quit_ = true;


    // Wait for all threads to join 
    for (auto& t : thread_pool_) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // Reset tasks completed for this run
    tasks_completed_.store(0);

    // Main thread executes here
    for (int task_id = 0; task_id < num_total_tasks; task_id+=1) {
        // try acquire the lock
        {
            // Lock is acquired 
            std::lock_guard<std::mutex> lock(lock_);

            // Push the object into the shared queue
            IRunnableWrapper new_task(runnable, task_id, num_total_tasks);
            tasks_.push(new_task);
        } // Lock is released here!
    }
    
    // monitor that all tasks are finished
    while (tasks_completed_.load() < num_total_tasks) {
        // spinning to wait
    }

    // Note: should not flag all threads should quit because threads should be reused
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

void TaskSystemParallelThreadPoolSleeping::thread_routine() {
    // Worker thread function
    while (true) {
        IRunnableWrapper task(nullptr, -1, 0);
        
        {
            // Wait for work to be available or for shutdown signal
            std::unique_lock<std::mutex> lock(this->lock_);
            
            // Wait until there's work to do or we need to quit
            this->work_available_.wait(lock, [this] {
                return !this->tasks_.empty() || this->threads_should_quit_;
            });
            
            // Check if we should quit
            if (this->threads_should_quit_) {
                return;
            }
            
            // Get a task if available
            if (!this->tasks_.empty()) {
                task = this->tasks_.front();
                this->tasks_.pop();
            }
        } // Release the lock
        
        // Execute the task if we got one
        if (task.runnable_ptr) {
            task.runnable_ptr->runTask(task.task_id, task.num_total_tasks);
            
            // Task finished, increment the counter and notify if all tasks are done
            {
                std::lock_guard<std::mutex> lock(this->lock_);
                this->tasks_completed_.fetch_add(1);
                this->all_tasks_done_.notify_all();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::thread_routine_async() {
    while (true) {
        IRunnableWrapperAsync subtask(nullptr, -1, 0, -1);

        {
            // Wait for work to be available or for shutdown signal
            std::unique_lock<std::mutex> lock(this->lock_);

            // Wait until there's work to do or we need to quit
            this->work_available_.wait(lock, [this] {
                return !this->working_queue.empty() || this->threads_should_quit_;
            });

            // Checkpoint: working queue not empty or system quit

            // check system should quit 
            if (this->threads_should_quit_) {
                return;
            }

            // get task if queue not empty
            if (!this->working_queue.empty()) {
                subtask = this->working_queue.front();
                this->working_queue.pop();
            }
        }   // release the lock 

        // execute task if we got one
        if (subtask.runnable_ptr) { 
            subtask.runnable_ptr->runTask(subtask.task_id, subtask.num_total_tasks);

            // increment the subtask completed
            int completed_subtask = task_numFinished[subtask.parent_task].fetch_add(1);
            
            // check whether task completed
            if (completed_subtask == subtask.num_total_tasks - 1) {
                {
                    // acquire the lock 
                    std::lock_guard<std::mutex> lock(lock_);

                    // mark the parent task completed
                    task_has_finished[subtask.parent_task] = true;
                    
                    // notify sync() that a task completed
                    all_tasks_done_.notify_all();

                    // check dependency 
                    for (const auto& dep : depended[subtask.parent_task]) {
                        // dep is a task that is depending on the current one 

                        bool dep_can_start = true;
                        
                        // check whether all deps' depending tasks are completed
                        for (const auto& dep_dependency : depending[dep]) {
                            if (!task_has_finished[dep_dependency]) {
                                dep_can_start = false;
                                break;
                            }
                        }
                        
                        // start the task if can start
                        if (dep_can_start) {
                            // init finished subtask number 
                            task_numFinished[dep].store(0);
            
                            TaskMetaData metadata = task_meta[dep];
                            
                            // push subtasks into working queue 
                            for (int subtask_id = 0; subtask_id < metadata.num_total_tasks; subtask_id++) {
                                IRunnableWrapperAsync new_task(metadata.runnable_ptr, subtask_id, metadata.num_total_tasks, dep);
                                working_queue.push(new_task);
                            }

                            // notify 
                            work_available_.notify_all();
                        }
                    }
                }
            }
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): 
    ITaskSystem(num_threads),
    num_threads_(num_threads),  
    threads_should_quit_(false),
    current_TaskID(0)
{
    // construct the thread pool
    for (int i = 0; i < num_threads; ++i) {
        thread_pool_.emplace_back([this] {
            this->thread_routine_async();
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    // Signal all threads to quit
    {
        std::lock_guard<std::mutex> lock(lock_);
        threads_should_quit_ = true;
    }
    
    // Wake up all worker threads so they can see the quit signal
    work_available_.notify_all();
    
    // Wait for all threads to join
    for (auto& t : thread_pool_) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // Implement run() using runAsyncWithDeps() and sync()
    std::vector<TaskID> noDeps;  // empty vector - no dependencies
    runAsyncWithDeps(runnable, num_total_tasks, noDeps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) 
{
    std::lock_guard<std::mutex> lock(lock_);
    
    // increment the taskid 
    TaskID id = current_TaskID.fetch_add(1);

    // Initialize task state
    task_has_finished[id] = false;
    task_numFinished[id].store(0);
    TaskMetaData metadata(runnable, id, num_total_tasks);
    task_meta[id] = metadata;

    // Set up dependency relationships
    depending[id] = deps;
    for (const auto& dep : deps) {
        depended[dep].push_back(id);
    }

    // Check if task can start immediately
    bool can_start = true;
    for (const auto& dep : deps) {
        if (!task_has_finished[dep]) {
            can_start = false;
            break;
        }
    }

    if (can_start) {
        // Push subtasks into working queue 
        for (int subtask_id = 0; subtask_id < num_total_tasks; subtask_id++) {
            IRunnableWrapperAsync new_task(runnable, subtask_id, num_total_tasks, id);
            working_queue.push(new_task);
        }
        work_available_.notify_all();
    } else {
        waiting_queue.push(id);
    }

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(lock_);
    
    all_tasks_done_.wait(lock, [this] {
        // check each task id 
        TaskID latest_id = current_TaskID.load();
        
        for (TaskID id = 0; id < latest_id; id++) {
            if (!task_has_finished[id]) {
                return false;
            }
        }
        return true;
    });
}
                                                                                                                                           