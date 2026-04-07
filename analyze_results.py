import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import pandas as pd

def load_metrics(experiment_name):
    file_path = f"results/{experiment_name}/metrics.json"
    if Path(file_path).exists():
        with open(file_path, 'r') as f:
            return json.load(f)
    return None

def create_comparison_charts():
    experiments = {
        "1 DataNode, Spark": "exp1_1dn_spark",
        "1 DataNode, Spark Opt": "exp2_1dn_opt",
        "3 DataNode, Spark": "exp3_3dn_spark",
        "3 DataNode, Spark Opt": "exp4_3dn_opt"
    }
    
    metrics_data = []
    for name, exp_id in experiments.items():
        metrics = load_metrics(exp_id)
        if metrics:
            metrics_data.append({
                "name": name,
                "duration": metrics.get("duration_seconds", 0),
                "memory": metrics.get("memory_used_mb", 0),
                "optimized": metrics.get("optimized", False),
                "data_nodes": 3 if "3 DataNode" in name else 1
            })
    
    if not metrics_data:
        print("No metrics data found. Run experiments first!")
        return
    
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
   
    ax1 = axes[0, 0]
    names = [d["name"] for d in metrics_data]
    durations = [d["duration"] for d in metrics_data]
    colors = ['#ff6b6b' if 'Opt' in n else '#4ecdc4' for n in names]
    bars = ax1.bar(names, durations, color=colors)
    ax1.set_ylabel('Time (seconds)', fontsize=12)
    ax1.set_title('Execution Time Comparison', fontsize=14, fontweight='bold')
    ax1.tick_params(axis='x', rotation=45)
    for bar, duration in zip(bars, durations):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{duration:.1f}s', ha='center', va='bottom', fontsize=10)
    
    
    ax2 = axes[0, 1]
    memory = [d["memory"] for d in metrics_data]
    bars = ax2.bar(names, memory, color=colors)
    ax2.set_ylabel('Memory (MB)', fontsize=12)
    ax2.set_title('Memory Usage Comparison', fontsize=14, fontweight='bold')
    ax2.tick_params(axis='x', rotation=45)
    for bar, mem in zip(bars, memory):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10,
                f'{mem:.0f} MB', ha='center', va='bottom', fontsize=10)
    
   
    ax3 = axes[1, 0]
    categories = ['1 DataNode', '3 DataNodes']
    spark_times = [metrics_data[0]["duration"], metrics_data[2]["duration"]]
    opt_times = [metrics_data[1]["duration"], metrics_data[3]["duration"]]
    
    x = np.arange(len(categories))
    width = 0.35
    bars1 = ax3.bar(x - width/2, spark_times, width, label='Standard Spark', color='#4ecdc4')
    bars2 = ax3.bar(x + width/2, opt_times, width, label='Optimized Spark', color='#ff6b6b')
    ax3.set_ylabel('Time (seconds)', fontsize=12)
    ax3.set_title('Standard vs Optimized Performance', fontsize=14, fontweight='bold')
    ax3.set_xticks(x)
    ax3.set_xticklabels(categories)
    ax3.legend()
    
    
    ax4 = axes[1, 1]
    speedup_1dn = metrics_data[0]["duration"] / metrics_data[1]["duration"]
    speedup_3dn = metrics_data[2]["duration"] / metrics_data[3]["duration"]
    speedups = [speedup_1dn, speedup_3dn]
    bars = ax4.bar(categories, speedups, color=['#45b7d1', '#96ceb4'])
    ax4.axhline(y=1, color='r', linestyle='--', label='Baseline (1x)')
    ax4.set_ylabel('Speedup Factor', fontsize=12)
    ax4.set_title('Optimization Speedup', fontsize=14, fontweight='bold')
    for bar, speed in zip(bars, speedups):
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                f'{speed:.2f}x', ha='center', va='bottom', fontsize=10)
    ax4.legend()
    
    plt.suptitle('Spark + Hadoop Performance Analysis Results', fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig('results/performance_analysis.png', dpi=150, bbox_inches='tight')
    plt.show()
    
    df = pd.DataFrame(metrics_data)
    df.to_csv('results/summary_table.csv', index=False)
    

if __name__ == "__main__":
    create_comparison_charts()