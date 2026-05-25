//! src/main.rs
//!
//! This Rust file is used to execute the entire program. Run this file to see everything in action.

use crate::config::{RunningTime, Safety};
use crate::monitor::CountingAllocator;
use std::io::{self, Write};

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

mod config;
mod display;
mod fault_tolerance;
mod ingest;
mod leaderboard;
mod monitor;
mod pipeline;
mod processor;
mod types;

/// The main class - Program running point
#[tokio::main]
async fn main() {

    // Remove everything from screen
    print!("\x1B[2J\x1B[1;1H");
    io::stdout().flush().unwrap();

    // Introductory line
    println!("\nWelcome to the real-time dashboard of Wikipedia Edits!");

    // Prompt for the different choices
    let architectural_choice = ask_architecture();
    let safety_choice = ask_safety();
    let time_choice = ask_time();

    // Branch to the corresponding program
    match architectural_choice.as_str() {

        // Async (Tokio) version
        "1" => {
            println!("Running task-based version ... ");

            // Error handling for async
            if let Err(error) = pipeline::runner::run_async_pipeline(safety_choice, time_choice).await {
                eprintln!("Error in running async version! Error: {}", error);
            }
        }

        // Threaded version
        "2" => {
            println!("Running thread-based version ... ");
            pipeline::runner::run_threaded_pipeline(safety_choice, time_choice);
        }

        // Should not branch here but included for completeness
        _ => {
            println!("System error. Please run the program again.");
            return;
        }
    }
}

/// A function to handle user input from CLI
fn prompt(message: &str) -> io::Result<String> {

    // Print the message, and force it to appear on CLI
    print!("{message}");
    io::stdout().flush()?;

    // Capture the text inputted by users
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    // Remove any trailing spaces and return the text
    Ok(input.trim().to_string())
}

/// A sub function to request user input for architectural choice
fn ask_architecture() -> String {

    // Prompt for architectural choice: Tokio or thread
    println!("\nPlease select the concurrency model that you wish to run:");
    println!("1. Task-based model (using Tokio)");
    println!("2. Thread-based model");
    let mut architecture_choice = prompt("\nInput your choice: ").expect("Input error");

    // Validation on architectural choice
    while architecture_choice != "1" && architecture_choice != "2" {

        // Empty input means exit
        if architecture_choice.is_empty() {
            println!("\nThank you for using the system. Exiting in progress ...");
            std::process::exit(0);
        }

        // If invalid input, then loop again
        println!("Error input. Please try again.");
        architecture_choice = prompt("\nInput your choice: ").expect("Input error for architecture choice!");
    }

    // Return the choice at last
    architecture_choice
}

/// A sub function to request user input for safety choice
fn ask_safety() -> Safety {
    
    // Prompt for safety features
    println!("\nPlease select the safety features that you wish to run:");
    println!("1. Mutex");
    println!("2. RwLock");
    println!("3. Atomic types");
    println!("4. DashMap");
    let mut safety_choice = prompt("\nInput your choice: ").expect("Input error");

    // Validation on safety features
    while safety_choice != "1"
        && safety_choice != "2"
        && safety_choice != "3"
        && safety_choice != "4"
    {
        // Empty means exit
        if safety_choice.is_empty() {
            println!("\nThank you for using the system. Exiting in progress ...");
            std::process::exit(0);
        }

        // Loop again for invalid input
        println!("Error input. Please try again.");
        safety_choice = prompt("\nInput your choice: ").expect("Input error for safety choice!");
    }

    // Convert the choice to enum at last
    Safety::convert_to_enum(&safety_choice).expect("Invalid safety choice!")
}

/// A sub function to request user input for time
fn ask_time() -> RunningTime {
    
    // Display time input
    println!("\nHow long do you wish to run the program:");
    println!("1. 30 seconds");
    println!("2. 60 seconds");
    println!("3. 90 seconds");
    let mut time_choice = prompt("\nInput your choice: ").expect("Input error");

    // Validation on safety features
    while time_choice != "1" && time_choice != "2" && time_choice != "3" {
        
        // Empty means exit
        if time_choice.is_empty() {
            println!("\nThank you for using the system. Exiting in progress ...");
            std::process::exit(0);
        }

        // Loop again for invalid input
        println!("Error input. Please try again.");
        time_choice = prompt("\nInput your choice: ").expect("Input error for safety choice!");
    }

    // Convert to enum at last
    RunningTime::convert_to_enum(&time_choice).expect("Invalid time choice!")
}
