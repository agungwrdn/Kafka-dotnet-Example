using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace Contoh_Kafka.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        private readonly IProducer<Null, string> producer;
        public ProducerController()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:19092", // Ganti dengan alamat dan port Kafka broker Anda
            };

            producer = new ProducerBuilder<Null, string>(config).Build();
        }
        [HttpPost]
        public async Task<ActionResult> ExamplePublisher()
        {
            var topic = "insert-tnt";
            var qrlis = new[]
            {
                new { id_qr = "AAAA" },
                new { id_qr = "BBBB"}
            };

            var data = new 
            {
                id_qr_log_header = "BLABLABLABLA",
                qr_list = qrlis
            };

            string json = JsonConvert.SerializeObject(data);

            try
            {
                var produceResult = await producer.ProduceAsync(topic, new Message<Null, string> { Value = json });

                Console.WriteLine($"Menghasilkan pesan ke {produceResult.Topic}: {produceResult.Value}");

                return Ok($"Menghasilkan pesan ke {produceResult.Topic}: {produceResult.Value}");
            }
            catch (Exception ex)
            {
                return BadRequest($"Gagal menghasilkan pesan: {ex.Message}");
            }

            return Ok(data);
        }
    }
}
