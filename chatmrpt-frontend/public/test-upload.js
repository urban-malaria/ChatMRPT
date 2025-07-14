// Browser console test script for file upload
// Copy and paste this into the browser console at http://localhost:3000

async function testUploadFunctionality() {
  console.log('Testing ChatMRPT file upload functionality...');
  
  // Test 1: Load sample data
  try {
    console.log('\n1. Testing sample data load...');
    const sampleBtn = document.querySelector('button:has-text("Load Sample Data")');
    if (sampleBtn) {
      sampleBtn.click();
      console.log('✓ Sample data button clicked');
    } else {
      console.log('Sample data button not found. Opening upload modal...');
      
      // Click the paperclip icon to open upload modal
      const uploadBtn = document.querySelector('[aria-label="Upload files"]');
      if (uploadBtn) {
        uploadBtn.click();
        await new Promise(resolve => setTimeout(resolve, 500));
        
        // Try to find and click sample data button
        const modalSampleBtn = document.querySelector('button:has-text("Load Sample Data")');
        if (modalSampleBtn) {
          modalSampleBtn.click();
          console.log('✓ Sample data button clicked in modal');
        }
      }
    }
  } catch (error) {
    console.error('Sample data test failed:', error);
  }
  
  // Test 2: Create and upload a test CSV file
  try {
    console.log('\n2. Testing CSV file upload...');
    
    // Create a test CSV blob
    const csvContent = `WardName,population,tpr,urbanPercentage,avg_rainfall,ndvi_score,dist_to_clinic,avg_house_score
Test Ward 1,15000,20.5,30.0,100.0,0.70,1000,0.5
Test Ward 2,25000,25.0,50.0,120.0,0.75,800,0.6
Test Ward 3,10000,15.0,20.0,90.0,0.65,1500,0.4`;
    
    const csvBlob = new Blob([csvContent], { type: 'text/csv' });
    const csvFile = new File([csvBlob], 'test_data.csv', { type: 'text/csv' });
    
    // Create a DataTransfer object to simulate file drop
    const dataTransfer = new DataTransfer();
    dataTransfer.items.add(csvFile);
    
    // Find the CSV dropzone
    const csvDropzone = document.querySelector('[data-testid="csv-dropzone"]');
    if (csvDropzone) {
      // Simulate file drop
      const dropEvent = new DragEvent('drop', {
        bubbles: true,
        cancelable: true,
        dataTransfer: dataTransfer
      });
      csvDropzone.dispatchEvent(dropEvent);
      console.log('✓ CSV file dropped');
    } else {
      console.log('CSV dropzone not found');
    }
  } catch (error) {
    console.error('CSV upload test failed:', error);
  }
  
  console.log('\nTest complete. Check the UI for results.');
}

// Run the test
testUploadFunctionality();