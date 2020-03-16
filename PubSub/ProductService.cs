using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using PubSub.DTO;
using System;
using System.Threading.Tasks;

namespace PubSub
{
    public class ProductService
    {
        public ProductService()
        {

        }

        // for ICollector implementation refer : 
        //https://docs.microsoft.com/en-us/azure/azure-functions/functions-dotnet-class-library#binding-at-runtime
        [FunctionName(nameof(AddNewProduct))]
        //[return: Queue("newproduct", Connection = "AzureWebJobsStorage")]
        public async Task<IActionResult> AddNewProduct(
            [HttpTrigger(AuthorizationLevel.Anonymous, "POST", Route = "addProduct")] HttpRequest req
            , [Queue("newProduct", Connection = "AzureWebJobsStorage")]ICollector<string> collector 
            , ILogger logger
            )
        {
            try
            {
                var json = await req.ReadAsStringAsync();
                var product = JsonConvert.DeserializeObject<Product>(json);

                //Fluent validation can be used. Leaving it for now.
                if (product == null || string.IsNullOrEmpty(product.Name.Trim()) || product.Price < 0)
                {
                    return new BadRequestResult();
                }
                collector.Add(json); //writes only when method executes succesfully
                return new CreatedResult("/api/addProduct", product);
            }
            catch(JsonReaderException e)
            {
                return new BadRequestObjectResult(e.Message);
            }
            catch (Exception e)
            {
                logger.LogError(e.ToString());
                return new ObjectResult("An error occurred at server.");
            }
           
        }
    }
}
