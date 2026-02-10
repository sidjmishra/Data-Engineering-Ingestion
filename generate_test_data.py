"""
Test Data Generator
Generates sample CSV, Image, and Video files for testing
"""

import os
import csv
from pathlib import Path
from PIL import Image
import numpy as np


def generate_test_csv():
    """Generate sample CSV file"""
    print("Generating test CSV file...")
    
    csv_path = "datasets/incoming/sample_sales_data.csv"
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    # Sample data
    rows = [
        ['OrderID', 'CustomerName', 'ProductName', 'Quantity', 'SaleAmount', 'OrderDate'],
        ['ORD001', 'John Smith', 'Laptop', '1', '1200.00', '2024-01-10'],
        ['ORD002', 'Jane Doe', 'Monitor', '2', '600.00', '2024-01-11'],
        ['ORD003', 'Bob Johnson', 'Keyboard', '5', '250.00', '2024-01-12'],
        ['ORD004', 'Alice Williams', 'Mouse', '10', '150.00', '2024-01-13'],
        ['ORD005', 'Charlie Brown', 'Headphones', '3', '200.00', '2024-01-14'],
        ['ORD006', 'Diana Prince', 'Webcam', '2', '100.00', '2024-01-15'],
        ['ORD007', 'Eve Davis', 'USB Hub', '4', '80.00', '2024-01-16'],
        ['ORD008', 'Frank Miller', 'Cable Set', '8', '40.00', '2024-01-17'],
        ['ORD009', 'Grace Lee', 'Speaker', '1', '150.00', '2024-01-18'],
        ['ORD010', 'Henry Wilson', 'Phone Stand', '6', '90.00', '2024-01-19'],
    ]
    
    # Write CSV
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    
    print(f"Created: {csv_path}")
    return csv_path


def generate_test_image():
    """Generate sample image file"""
    print("Generating test image file...")
    
    image_path = "datasets/incoming/sample_image.jpg"
    os.makedirs(os.path.dirname(image_path), exist_ok=True)
    
    # Create colorful gradient image
    width, height = 1920, 1080
    
    # Create RGB image with gradient
    img_array = np.zeros((height, width, 3), dtype=np.uint8)
    
    # Red gradient horizontal
    for x in range(width):
        img_array[:, x, 0] = int(255 * (x / width))
    
    # Green gradient vertical
    for y in range(height):
        img_array[y, :, 1] = int(255 * (y / height))
    
    # Blue constant
    img_array[:, :, 2] = 128
    
    # Create and save image
    img = Image.fromarray(img_array, 'RGB')
    img.save(image_path, quality=95)
    
    print(f"Created: {image_path}")
    return image_path


def generate_test_image_png():
    """Generate sample PNG image file"""
    print("Generating test PNG image file...")
    
    image_path = "datasets/incoming/sample_chart.png"
    os.makedirs(os.path.dirname(image_path), exist_ok=True)
    
    # Create PNG with transparency
    width, height = 800, 600
    
    # Create RGBA image
    img_array = np.zeros((height, width, 4), dtype=np.uint8)
    
    # White background with some colors
    img_array[:, :, 0] = 255  # Red channel
    img_array[:, :, 1] = 255  # Green channel
    img_array[:, :, 2] = 200  # Blue channel
    img_array[:, :, 3] = 255  # Alpha channel (fully opaque)
    
    # Add some gradient
    for y in range(height):
        img_array[y, :, 3] = int(255 * (y / height))  # Gradient transparency
    
    img = Image.fromarray(img_array, 'RGBA')
    img.save(image_path)
    
    print(f"Created: {image_path}")
    return image_path


def main():
    """Generate all test data"""
    print("=" * 60)
    print("TEST DATA GENERATOR")
    print("=" * 60)
    print()
    
    try:
        # Change to project root if running from src
        if os.path.exists('src'):
            os.chdir(os.path.dirname(__file__))
        
        print("Generating sample test files...\n")
        
        # Generate test files
        csv_file = generate_test_csv()
        image_jpg = generate_test_image()
        image_png = generate_test_image_png()
        
        print()
        print("=" * 60)
        print("TEST DATA GENERATED SUCCESSFULLY")
        print("=" * 60)
        print()
        print("Files created in: datasets/incoming/")
        print()
        print("Next steps:")
        print("1. Start the ingestion pipeline: python main.py")
        print("2. Wait for processing (check logs in real-time)")
        print("3. Verify files in:")
        print("   - datasets/raw/")
        print("   - datasets/validated/")
        print("4. Check database for metadata")
        print()
        
    except Exception as e:
        print(f"Error generating test data: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
