using System;
using System.Diagnostics;
using System.Net;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using ContosoAdsCommon;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using Microsoft.Azure;
using Newtonsoft.Json;

namespace ContosoAdsWorkerWriteImage
{
    public class WriteImageWorker : RoleEntryPoint
    {
        private CloudQueue imagesQueue;
        private CloudBlobContainer imagesBlobContainer;
        private ContosoAdsContext db;
        public override void Run()
        {
            Trace.TraceInformation("WriteImageWorker entry point called");
            CloudQueueMessage msg = null;

            while (true)
            {
                try
                {
                    msg = this.imagesQueue.GetMessage();
                    if (msg != null)
                    {
                        ProcessQueueMessage(msg);
                    }
                    else
                    {
                        System.Threading.Thread.Sleep(1000);
                    }
                }
                catch (StorageException e)
                {
                    if (msg != null && msg.DequeueCount > 5)
                    {
                        this.imagesQueue.DeleteMessage(msg);
                        Trace.TraceError("Deleting poison queue item: '{0}'", msg.AsString);
                    }
                    Trace.TraceError("Exception in ContosoAdsWorker: '{0}'", e.Message);
                    System.Threading.Thread.Sleep(5000);
                }
            }
        }
        private void ProcessCreateWrittenImage(int adId, CloudQueueMessage msg)
        {
            Ad ad = db.Ads.Find(adId);
            if (ad == null)
            {
                throw new Exception(String.Format("AdId {0} not found, can't create thumbnail", adId.ToString()));
            }
            Uri blobUri = new Uri(ad.ImageURL);
            string blobName = blobUri.Segments[blobUri.Segments.Length - 1];

            CloudBlockBlob inputBlob = this.imagesBlobContainer.GetBlockBlobReference(blobName);
            string writtenImageName = Path.GetFileNameWithoutExtension(inputBlob.Name) + "written.jpg";
            CloudBlockBlob outputBlob = this.imagesBlobContainer.GetBlockBlobReference(writtenImageName);

            using (Stream input = inputBlob.OpenRead())
            using (Stream output = outputBlob.OpenWrite())
            {
                WriteToImage(input, output);
                outputBlob.Properties.ContentType = "image/jpeg";
            }
            Trace.TraceInformation("Generated thumbnail in blob {0}", writtenImageName);
        }
        private void ProcessQueueMessage(CloudQueueMessage msg)
        {
            Trace.TraceInformation("Processing queue message {0}", msg);

            // Queue message contains AdId.
            AdQueue adQueue = JsonConvert.DeserializeObject<AdQueue>(msg.AsString);
            if (adQueue.HasWrittenImageURL == false)
            {
                var adId = adQueue.AdId;
                ProcessCreateWrittenImage(adId, msg);
                // Remove message from queue.
                this.imagesQueue.DeleteMessage(msg);
                adQueue.HasWrittenImageURL = true;
                if (adQueue.HasThumbnailURL == false)
                {
                    var queueMessage = new CloudQueueMessage(JsonConvert.SerializeObject(adQueue));
                    imagesQueue.BeginAddMessage(queueMessage, null, null);
                }
            }
            
        }

        public void WriteToImage(Stream input, Stream output)
        {
            var originalImage = new Bitmap(input);
            var writtenImage = new Bitmap(originalImage.Width, originalImage.Height);
            
            try
            {
                Font font = new Font("Arial", 20, FontStyle.Italic, GraphicsUnit.Pixel);
                Color color = Color.White;
                SolidBrush brush = new SolidBrush(color);
                StringFormat sf = new StringFormat();
                sf.Alignment = StringAlignment.Center;
                sf.LineAlignment = StringAlignment.Center;

                Point atpoint = new Point(10, writtenImage.Height - 10);
                using (Graphics graphics = Graphics.FromImage(writtenImage))
                {
                    //graphics.InterpolationMode = InterpolationMode.HighQualityBicubic;
                    //graphics.SmoothingMode = SmoothingMode.AntiAlias;
                    //graphics.PixelOffsetMode = PixelOffsetMode.HighQuality;

                    graphics.DrawImage(originalImage, 0, 0, writtenImage.Width, writtenImage.Height);
                    graphics.DrawString(DateTime.Now.ToString(), font, brush, atpoint, sf);
                }

                writtenImage.Save(output, ImageFormat.Jpeg);
            }
            finally
            {
                if (writtenImage != null)
                {
                    writtenImage.Dispose();
                }
            }
        }

        // A production app would also include an OnStop override to provide for
        // graceful shut-downs of worker-role VMs.  See
        // http://azure.microsoft.com/en-us/documentation/articles/cloud-services-dotnet-multi-tier-app-storage-3-web-role/#restarts
        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections.
            ServicePointManager.DefaultConnectionLimit = 12;

            // Read database connection string and open database.
            var dbConnString = CloudConfigurationManager.GetSetting("ContosoAdsDbConnectionString");
            db = new ContosoAdsContext(dbConnString);

            // Open storage account using credentials from .cscfg file.
            var storageAccount = CloudStorageAccount.Parse
                (RoleEnvironment.GetConfigurationSettingValue("StorageConnectionString"));

            Trace.TraceInformation("Creating images blob container");
            var blobClient = storageAccount.CreateCloudBlobClient();
            imagesBlobContainer = blobClient.GetContainerReference("images");
            if (imagesBlobContainer.CreateIfNotExists())
            {
                // Enable public access on the newly created "images" container.
                imagesBlobContainer.SetPermissions(
                    new BlobContainerPermissions
                    {
                        PublicAccess = BlobContainerPublicAccessType.Blob
                    });
            }

            Trace.TraceInformation("Creating images queue");
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            imagesQueue = queueClient.GetQueueReference("images");
            imagesQueue.CreateIfNotExists();

            Trace.TraceInformation("Storage initialized");
            return base.OnStart();
        }
    }
}
