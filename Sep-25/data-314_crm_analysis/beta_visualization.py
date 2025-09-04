import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import beta

def visualize_beta(alpha, beta_param):
    x = np.linspace(0, 1, 1000)
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    
    # PDF
    axes[0].plot(x, beta.pdf(x, alpha, beta_param), 'b-', linewidth=2)
    axes[0].set_title(f'Beta PDF (α={alpha}, β={beta_param})')
    axes[0].set_xlabel('x')
    axes[0].set_ylabel('f(x)')
    axes[0].grid(True, alpha=0.3)
    
    # CDF
    axes[1].plot(x, beta.cdf(x, alpha, beta_param), 'r-', linewidth=2)
    axes[1].set_title(f'Beta CDF (α={alpha}, β={beta_param})')
    axes[1].set_xlabel('x')
    axes[1].set_ylabel('F(x)')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()
    
    mean_val = alpha / (alpha + beta_param)
    var_val = (alpha * beta_param) / ((alpha + beta_param)**2 * (alpha + beta_param + 1))
    
    print(f"평균: {mean_val:.4f}, 분산: {var_val:.4f}")
    
    return fig
